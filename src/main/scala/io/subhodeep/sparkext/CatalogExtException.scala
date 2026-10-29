package io.subhodeep.sparkext

import org.apache.spark.sql.AnalysisException

/**
 * Error raised by this extension. Messages never contain option values (see [[Redact]]).
 *
 * AnalysisException's message-only constructor is `protected[sql]`; subclassing is the supported
 * way to reach it from outside Spark without inventing an error class in Spark's registry.
 */
class CatalogExtException(message: String, cause: Option[Throwable] = None)
  extends AnalysisException(
    s"[CATALOGEXT] $message",
    None,
    None,
    cause,
    None,
    Map.empty[String, String],
    Array.empty)
