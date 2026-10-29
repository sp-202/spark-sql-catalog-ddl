package io.subhodeep.sparkext

import java.lang.reflect.Field

import scala.collection.mutable
import scala.util.Try

import org.apache.spark.sql.SparkSession

/**
 * FRAGILE: the only place that touches Spark internals.
 *
 * Spark 3.5 has no public API to unregister a loaded CatalogPlugin. `CatalogManager` is
 * `private[sql]` and caches plugins in `private val catalogs = mutable.HashMap[String, CatalogPlugin]`
 * guarded by `this.synchronized`. We read that field reflectively and mutate it while holding the
 * same monitor. If a Spark build renames the field, changes its type or its locking, this breaks;
 * callers must handle the exception (DROP CATALOG reports it, CREATE CATALOG only logs it).
 * Verified against Apache Spark 3.5.8 and the custom 3.5.8 build (see README, Compatibility).
 */
private[sparkext] object CatalogManagerAccess {
  private val ClassName = "org.apache.spark.sql.connector.catalog.CatalogManager"
  private val FieldName = "catalogs"

  private lazy val catalogsField: Try[Field] = Try {
    val f = Class.forName(ClassName).getDeclaredField(FieldName)
    f.setAccessible(true)
    f
  }

  // The static type is private[sql]; we only need it as a monitor and a reflection target.
  private def manager(session: SparkSession): AnyRef = session.sessionState.catalogManager

  private def catalogsOf(cm: AnyRef): mutable.Map[String, AnyRef] =
    catalogsField.get.get(cm) match {
      case m: mutable.Map[_, _] => m.asInstanceOf[mutable.Map[String, AnyRef]]
      case other => throw new IllegalStateException(
        s"$ClassName.$FieldName has unexpected type ${Option(other).map(_.getClass.getName).orNull}")
    }

  /** True if a plugin instance named `name` is currently cached. Throws if reflection fails. */
  def isCached(session: SparkSession, name: String): Boolean = {
    val cm = manager(session)
    cm.synchronized(catalogsOf(cm).contains(name))
  }

  /** Removes the cached plugin, returning true if one was present. Throws if reflection fails. */
  def evict(session: SparkSession, name: String): Boolean = {
    val cm = manager(session)
    cm.synchronized(catalogsOf(cm).remove(name).isDefined)
  }
}
