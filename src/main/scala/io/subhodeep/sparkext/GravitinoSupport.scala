package io.subhodeep.sparkext

import scala.annotation.tailrec
import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession

/**
 * Apache Gravitino 1.3.x integration (gravitino-spark-connector-runtime-3.5_2.13).
 *
 * With `GravitinoSparkPlugin` in `spark.plugins`, Gravitino registers one Spark catalog per
 * relational catalog of the metalake at startup, and its catalog classes fetch their properties
 * from the Gravitino server by name when Spark first loads them. `CREATE CATALOG` can therefore
 * attach a catalog that was created in Gravitino after the application started:
 * {{{
 *   CREATE CATALOG sales USING GRAVITINO;   -- class resolved from the catalog's provider
 *   CREATE CATALOG sales USING 'org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark35';
 * }}}
 * Before writing conf we look the catalog up in Gravitino (a metadata call; the Spark catalog is
 * still loaded lazily) so a missing plugin, unknown catalog or provider/class mismatch fails here
 * rather than on first use.
 *
 * Gravitino classes are referenced only from [[GravitinoBridge]], which runs only after the plugin
 * check, so this jar keeps working when the Gravitino connector is not on the classpath.
 */
private[sparkext] object GravitinoSupport {
  /** `USING GRAVITINO` (bare keyword or quoted) asks Gravitino for the catalog class. */
  val Keyword = "GRAVITINO"

  val PluginClass = "org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin"
  private val ConnectorPackage = "org.apache.gravitino.spark.connector."
  private val EnableIcebergKey = "spark.sql.gravitino.enableIcebergSupport"
  private val EnablePaimonKey = "spark.sql.gravitino.enablePaimonSupport"
  private val MetalakeKey = "spark.sql.gravitino.metalake"

  /** Gravitino's built-in Spark 3.5 catalogs (CatalogNameAdaptor in 1.3.x). */
  val CatalogClasses: Seq[String] = Seq(
    "iceberg.GravitinoIcebergCatalogSpark35",
    "hive.GravitinoHiveCatalogSpark35",
    "paimon.GravitinoPaimonCatalogSpark35",
    "glue.GravitinoGlueCatalogSpark35",
    "jdbc.GravitinoJdbcCatalogSpark35",
    "jdbc.postgresql.GravitinoPostgreSqlCatalogSpark35").map(ConnectorPackage + _)

  def isKeyword(impl: String): Boolean = impl.equalsIgnoreCase(Keyword)

  def isConnectorClass(impl: String): Boolean = impl.startsWith(ConnectorPackage)

  def pluginEnabled(session: SparkSession): Boolean = {
    val conf = session.sparkContext.getConf
    Seq("spark.plugins", "spark.plugins.defaultList")
      .flatMap(conf.getOption)
      .flatMap(_.split(","))
      .exists(_.trim == PluginClass)
  }

  /**
   * Returns the Spark catalog class Gravitino binds to catalog `name`, checking that it equals
   * `requested` when one was given. Throws CatalogExtException with a specific reason otherwise.
   */
  def resolveCatalogClass(session: SparkSession, name: String, requested: Option[String]): String = {
    if (!pluginEnabled(session)) {
      throw new CatalogExtException(
        s"Catalog '$name' uses Gravitino, but $PluginClass is not in spark.plugins.")
    }
    val conf = session.sparkContext.getConf
    val metalake = conf.getOption(MetalakeKey).getOrElse("?")
    def missing = new CatalogExtException(
      s"Catalog '$name' does not exist in Gravitino metalake '$metalake' (or is not a relational catalog).")
    val (provider, resolved) = try GravitinoBridge.lookup(name).getOrElse(throw missing) catch {
      case e: CatalogExtException => throw e
      case NonFatal(e) if rootCause(e).getClass.getSimpleName == "NoSuchCatalogException" => throw missing
      case e: LinkageError =>
        throw new CatalogExtException(
          s"Gravitino connector classes are not loadable (${e.getClass.getSimpleName}); add " +
            "gravitino-spark-connector-runtime-3.5_2.13 to --jars.")
      case NonFatal(e) =>
        throw new CatalogExtException(
          s"Cannot look up catalog '$name' in Gravitino metalake '$metalake': ${describe(rootCause(e))}",
          Some(e))
    }

    // Mirror GravitinoDriverPlugin, which skips these providers unless support is enabled.
    def requireFlag(key: String): Unit =
      if (!conf.getBoolean(key, defaultValue = false)) {
        throw new CatalogExtException(
          s"Gravitino catalog '$name' has provider '$provider', which needs $key=true.")
      }
    if (provider.startsWith("lakehouse-iceberg")) requireFlag(EnableIcebergKey)
    if (provider.startsWith("lakehouse-paimon")) requireFlag(EnablePaimonKey)

    val cls = resolved.getOrElse(throw new CatalogExtException(
      s"Gravitino catalog '$name' has provider '$provider', which has no Spark catalog binding."))
    requested.filter(_ != cls).foreach { r =>
      throw new CatalogExtException(
        s"Gravitino catalog '$name' has provider '$provider', which binds to '$cls', not '$r'. " +
          "Use USING GRAVITINO to pick the class automatically.")
    }
    cls
  }

  @tailrec private def rootCause(t: Throwable): Throwable =
    if (t.getCause == null || (t.getCause eq t)) t else rootCause(t.getCause)

  private def describe(t: Throwable): String =
    s"${t.getClass.getSimpleName}: ${Option(t.getMessage).getOrElse("").take(300)}"
}

/** The only code that links against Gravitino classes. */
private object GravitinoBridge {
  import org.apache.gravitino.Catalog
  import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager
  import org.apache.gravitino.spark.connector.version.CatalogNameAdaptor

  /**
   * Some((provider, Spark catalog class if Gravitino binds one)), or None if the metalake has no
   * relational catalog of that name.
   *
   * getGravitinoCatalogInfo logs a full ERROR stack trace for a missing catalog before rethrowing.
   * So on a cache miss we first refresh the manager's cache from the server's catalog list
   * (loadRelationalCatalogs, which also picks up catalogs created after startup), and only load a
   * name that the listing contains.
   */
  def lookup(name: String): Option[(String, Option[String])] = {
    val cm = try GravitinoCatalogManager.get() catch {
      case e: IllegalStateException => // "has not created yet" / "already closed"
        throw new CatalogExtException(s"Gravitino plugin is not initialized: ${e.getMessage}")
    }
    if (!cm.getCatalogs.containsKey(name)) cm.loadRelationalCatalogs()
    if (!cm.getCatalogs.containsKey(name)) return None

    val info = cm.getGravitinoCatalogInfo(name)
    if (info.`type`() != Catalog.Type.RELATIONAL) return None
    val provider = Option(info.provider()).getOrElse("")
    val cls = if (provider.isEmpty) None else Option(CatalogNameAdaptor.getCatalogName(provider))
    Some((provider, cls.filter(_.nonEmpty)))
  }
}
