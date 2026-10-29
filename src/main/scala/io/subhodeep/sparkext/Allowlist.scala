package io.subhodeep.sparkext

import org.apache.spark.sql.SparkSession

/**
 * Catalog implementation classes that CREATE CATALOG may register.
 *
 * Read from the application's static SparkConf, not the session conf, so a SQL user cannot widen
 * it with `SET spark.sql.catalogext.allowedClasses=...`. When the conf is not set and the Gravitino
 * plugin is enabled, Gravitino's built-in catalog classes are allowed too: enabling the plugin
 * already opts in to them. An explicit setting is always used as-is.
 */
private[sparkext] object Allowlist {
  val ConfKey = "spark.sql.catalogext.allowedClasses"
  val Default: Seq[String] = Seq(
    "org.apache.iceberg.spark.SparkCatalog",
    "org.apache.iceberg.spark.SparkSessionCatalog")

  def allowed(session: SparkSession): Seq[String] =
    session.sparkContext.getConf.getOption(ConfKey) match {
      case Some(v) => v.split(",").map(_.trim).filter(_.nonEmpty).toSeq
      case None if GravitinoSupport.pluginEnabled(session) => Default ++ GravitinoSupport.CatalogClasses
      case None => Default
    }

  def check(session: SparkSession, impl: String): Unit = {
    val classes = allowed(session)
    if (!classes.contains(impl)) {
      throw new CatalogExtException(
        s"Catalog class '$impl' is not allowed. Allowed classes: ${classes.mkString("[", ", ", "]")}. " +
          s"Set $ConfKey at application launch to change this; it cannot be changed at runtime.")
    }
  }
}
