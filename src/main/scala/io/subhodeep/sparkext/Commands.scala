package io.subhodeep.sparkext

import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StringType

private[sparkext] object CatalogCommands {
  val SessionCatalogName = "spark_catalog"
  val SessionCatalogDefaultClass = "org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog"

  private val NamePattern = "^[A-Za-z_][A-Za-z0-9_]*$".r
  private val OptionKeyPattern = "^[A-Za-z0-9_\\-]+(\\.[A-Za-z0-9_\\-]+)*$".r

  def confKey(name: String): String = s"spark.sql.catalog.$name"

  def validateName(name: String): Unit = {
    if (NamePattern.findFirstIn(name).isEmpty) {
      throw new CatalogExtException(
        s"Invalid catalog name '$name': must match ${NamePattern.regex}.")
    }
    if (name.equalsIgnoreCase(SessionCatalogName)) {
      throw new CatalogExtException(s"'$SessionCatalogName' is reserved and cannot be managed here.")
    }
  }

  def validateOptionKeys(options: Seq[(String, String)]): Unit = {
    options.foreach { case (k, _) =>
      if (OptionKeyPattern.findFirstIn(k).isEmpty) {
        throw new CatalogExtException(s"Invalid option key '$k': must match ${OptionKeyPattern.regex}.")
      }
    }
    // Spark hands options to the plugin as a CaseInsensitiveStringMap, so these would collide.
    options.groupBy(_._1.toLowerCase(Locale.ROOT)).collectFirst {
      case (k, dups) if dups.size > 1 => throw new CatalogExtException(s"Duplicate option key '$k'.")
    }
  }

  def isDefined(session: SparkSession, name: String): Boolean =
    session.conf.getOption(confKey(name)).isDefined
}

// Commands log through Spark's Logging trait (inherited `log`), under their own class names.

/**
 * CREATE CATALOG [IF NOT EXISTS] name USING 'class' | GRAVITINO [OPTIONS (...)].
 * Registers the catalog through session conf only; Spark loads the plugin lazily on first use.
 */
case class CreateCatalogCmd(
    name: String,
    impl: String,
    options: Seq[(String, String)],
    ifNotExists: Boolean) extends LeafRunnableCommand {
  import CatalogCommands._

  // Plan strings end up in the UI, event logs and EXPLAIN output.
  override protected def stringArgs: Iterator[Any] =
    Iterator(name, impl, Redact.render(options), ifNotExists)

  override def run(session: SparkSession): Seq[Row] = {
    validateName(name)
    val fromGravitino = GravitinoSupport.isKeyword(impl)
    if (!fromGravitino) Allowlist.check(session, impl)
    validateOptionKeys(options)

    if (isDefined(session, name)) {
      if (ifNotExists) {
        log.info(s"Catalog '$name' already exists; IF NOT EXISTS given, nothing to do.")
        return Seq.empty
      }
      throw new CatalogExtException(
        s"Catalog '$name' already exists. Use CREATE CATALOG IF NOT EXISTS or DROP CATALOG first.")
    }

    // Gravitino catalogs are checked against the server now instead of failing on first use.
    val catalogClass =
      if (fromGravitino) GravitinoSupport.resolveCatalogClass(session, name, None)
      else if (GravitinoSupport.isConnectorClass(impl)) {
        GravitinoSupport.resolveCatalogClass(session, name, Some(impl))
      } else impl
    if (fromGravitino) Allowlist.check(session, catalogClass)

    // Resolve everything before touching conf so a missing env var leaves no partial state.
    val resolved = options.map { case (k, v) => k -> EnvSubstitution.resolve(k, v) }

    // A plugin can only be cached without conf if an earlier DROP could not evict it.
    try {
      if (CatalogManagerAccess.evict(session, name)) {
        log.warn(s"Evicted a stale cached instance of catalog '$name'.")
      }
    } catch {
      case NonFatal(e) =>
        log.warn(s"Could not check for a stale cached instance of catalog '$name': $e")
    }

    session.conf.set(confKey(name), catalogClass)
    resolved.foreach { case (k, v) => session.conf.set(s"${confKey(name)}.$k", v) }
    log.info(s"Registered catalog '$name' using $catalogClass with options ${Redact.render(options)}")

    // EventLoggingListener redacts job/stage properties only for keys in the static SparkConf, and
    // SQLExecution copies every session conf into those properties. Runtime secrets therefore reach
    // the event log in clear text; this is Spark behaviour we cannot change from an extension.
    val sensitive = options.map(_._1).filter(Redact.isSensitive)
    if (sensitive.nonEmpty && session.sparkContext.getConf.getBoolean("spark.eventLog.enabled", false)) {
      log.warn(s"Catalog '$name' options ${sensitive.mkString("[", ", ", "]")} look sensitive. " +
        "With spark.eventLog.enabled, Spark writes runtime session conf unredacted into event-log " +
        "job properties; pass secrets as static --conf spark.sql.catalog.<name>.<key> instead.")
    }
    Seq.empty
  }
}

/** DROP CATALOG [IF EXISTS] name. Removes the conf keys and evicts the cached plugin. */
case class DropCatalogCmd(name: String, ifExists: Boolean) extends LeafRunnableCommand {
  import CatalogCommands._

  override def run(session: SparkSession): Seq[Row] = {
    validateName(name)
    val cached = try CatalogManagerAccess.isCached(session, name) catch { case NonFatal(_) => false }
    if (!isDefined(session, name) && !cached) {
      if (ifExists) return Seq.empty
      throw new CatalogExtException(s"Catalog '$name' not found.")
    }
    if (session.catalog.currentCatalog() == name) {
      throw new CatalogExtException(
        s"Cannot drop the current catalog '$name'. Switch with USE <other catalog> first.")
    }

    val prefix = confKey(name)
    session.conf.getAll.keys
      .filter(k => k == prefix || k.startsWith(prefix + "."))
      .foreach(session.conf.unset)

    try {
      CatalogManagerAccess.evict(session, name)
    } catch {
      case NonFatal(e) =>
        throw new CatalogExtException(
          s"Catalog '$name' config was removed, but its cached instance could not be evicted " +
            s"(${e.getClass.getName}); it stays usable until the session ends.", Some(e))
    }
    log.info(s"Dropped catalog '$name'.")
    Seq.empty
  }
}

/** SHOW CATALOGS: every catalog configured in session conf plus spark_catalog, with its class. */
case class ShowCatalogsCmd() extends LeafRunnableCommand {
  import CatalogCommands._

  override val output: Seq[Attribute] = Seq(
    AttributeReference("catalog", StringType, nullable = false)(),
    AttributeReference("class", StringType, nullable = false)())

  override def run(session: SparkSession): Seq[Row] = {
    val prefix = "spark.sql.catalog."
    val configured = session.conf.getAll.collect {
      case (k, v) if k.startsWith(prefix) && k.length > prefix.length &&
        !k.substring(prefix.length).contains('.') => k.substring(prefix.length) -> v
    }
    val sessionCatalog = SessionCatalogName ->
      configured.getOrElse(SessionCatalogName, SessionCatalogDefaultClass)
    (configured + sessionCatalog).toSeq.sortBy(_._1).map { case (n, c) => Row(n, c) }
  }
}
