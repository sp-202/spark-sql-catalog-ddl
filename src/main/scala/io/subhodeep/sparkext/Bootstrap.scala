package io.subhodeep.sparkext

import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession

/**
 * Phase 2: registers catalogs listed in the JSON file named by `spark.sql.catalogext.bootstrapFile`
 * (static conf), once per session, on the first parsePlan call. Each entry goes through
 * CreateCatalogCmd with IF NOT EXISTS, so catalogs already set via --conf win.
 * If loading fails the error is thrown and retried on the next statement, so a broken file is
 * never silently ignored.
 */
private[sparkext] class Bootstrap(session: SparkSession) {
  @volatile private var done = false

  def ensureLoaded(): Unit = if (!done) synchronized {
    if (!done) {
      Bootstrap.path(session).foreach { p =>
        Bootstrap.readEntries(p).foreach(_.run(session))
        Bootstrap.log.info(s"Loaded catalog bootstrap file $p")
      }
      done = true
    }
  }
}

private[sparkext] object Bootstrap {
  private val log = LoggerFactory.getLogger(classOf[Bootstrap])
  val ConfKey = "spark.sql.catalogext.bootstrapFile"
  private val EntryFields = Set("name", "impl", "options")

  def path(session: SparkSession): Option[Path] =
    session.sparkContext.getConf.getOption(ConfKey).map(_.trim).filter(_.nonEmpty).map(Paths.get(_))

  /** Parses the file into commands. Error messages name fields, never values. */
  def readEntries(p: Path): Seq[CreateCatalogCmd] = {
    def bad(msg: String) = new CatalogExtException(s"Invalid bootstrap file $p: $msg")

    val root: JsonNode = try {
      new ObjectMapper().readTree(Files.readString(p))
    } catch {
      // Jackson messages quote the offending input, which may be a secret.
      case e: JsonProcessingException =>
        val loc = Option(e.getLocation).map(l => s" at line ${l.getLineNr}, column ${l.getColumnNr}")
        throw bad(s"malformed JSON${loc.getOrElse("")}.")
      case e: java.io.IOException => throw bad(s"cannot read file (${e.getClass.getSimpleName}).")
    }
    if (root == null || !root.isArray) throw bad("top level must be a JSON array.")

    root.elements().asScala.zipWithIndex.map { case (e, idx) =>
      val at = s"entry #$idx"
      if (!e.isObject) throw bad(s"$at must be an object.")
      e.fieldNames().asScala.find(!EntryFields.contains(_)).foreach { f =>
        throw bad(s"$at has unknown field '$f' (expected ${EntryFields.mkString(", ")}).")
      }
      def text(f: String): String = {
        val v = e.get(f)
        if (v == null || !v.isTextual) throw bad(s"$at: '$f' must be a string.")
        v.asText()
      }
      val options = Option(e.get("options")) match {
        case None => Nil
        case Some(o) if o.isObject =>
          o.fields().asScala.map { f =>
            if (!f.getValue.isTextual) throw bad(s"$at: 'options.${f.getKey}' must be a string.")
            f.getKey -> f.getValue.asText()
          }.toSeq
        case Some(_) => throw bad(s"$at: 'options' must be an object.")
      }
      CreateCatalogCmd(text("name"), text("impl"), options, ifNotExists = true)
    }.toSeq
  }
}
