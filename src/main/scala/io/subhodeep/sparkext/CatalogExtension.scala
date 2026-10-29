package io.subhodeep.sparkext

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Entry point for `spark.sql.extensions`. Only wraps the parser; injects no rules, so it does not
 * interact with planner extensions such as Comet or Iceberg's optimizer rules.
 *
 * (Unrelated to Spark's own `org.apache.spark.sql.connector.catalog.CatalogExtension`.)
 */
class CatalogExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit =
    extensions.injectParser((session, delegate) => new CatalogExtParser(session, delegate))
}
