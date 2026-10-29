package io.subhodeep.sparkext

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Paths}

import scala.collection.mutable
import scala.util.Try

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Integration tests against a running Gravitino 1.3.x server with its Iceberg REST service, e.g.
 * the podman stack in dev-stack/ (`dev-stack/stack.sh up`). Skipped unless both of the first two
 * variables are set:
 *
 *  - GRAVITINO_IT_URI           Gravitino API, e.g. http://127.0.0.1:8090
 *  - GRAVITINO_IT_CATALOG_PROPS JSON file of lakehouse-iceberg catalog properties in which
 *                               "__NAME__" is replaced by the catalog name
 *                               (dev-stack/conf/catalog-props-template.json)
 *  - GRAVITINO_IT_METALAKE      metalake served by the Iceberg REST service (default "lake")
 *  - GRAVITINO_IT_IRC_URI       Iceberg REST endpoint (default http://127.0.0.1:9001/iceberg/)
 *
 * Catalogs get a per-run suffix and are deleted afterwards; the metalake is left alone.
 */
class GravitinoSuite extends AnyFunSuite with BeforeAndAfterAll {
  private val serverUri = sys.env.get("GRAVITINO_IT_URI").map(_.stripSuffix("/"))
  private val propsTemplate = sys.env.get("GRAVITINO_IT_CATALOG_PROPS")
    .map(p => new String(Files.readAllBytes(Paths.get(p))))
  private val metalake = sys.env.getOrElse("GRAVITINO_IT_METALAKE", "lake")
  private val ircUri = sys.env.getOrElse("GRAVITINO_IT_IRC_URI", "http://127.0.0.1:9001/iceberg/")
  private val enabled = serverUri.isDefined && propsTemplate.isDefined

  private val run = (System.currentTimeMillis() / 1000 % 1000000).toString
  private val pre = s"it_pre_$run"
  private val late = s"it_late_$run"
  private val explicit = s"it_explicit_$run"
  private val irc = s"it_irc_$run"
  private val IcebergGravitino = GravitinoSupport.CatalogClasses.head
  private val HiveGravitino = GravitinoSupport.CatalogClasses(1)

  private var spark: SparkSession = _
  private val http = HttpClient.newHttpClient()
  private val created = mutable.ArrayBuffer.empty[String]

  private def sql(s: String): DataFrame = spark.sql(s)
  private def errorOf(s: String): String = intercept[Exception](sql(s).collect()).getMessage
  private def catalogs: Map[String, String] =
    sql("SHOW CATALOGS").collect().map(r => r.getString(0) -> r.getString(1)).toMap
  private def count(table: String): Long = sql(s"SELECT count(*) FROM $table").collect().head.getLong(0)

  private def call(method: String, path: String, body: String = null): String = {
    val b = HttpRequest.newBuilder(URI.create(serverUri.get + path))
      .header("Accept", "application/vnd.gravitino.v1+json")
      .header("Content-Type", "application/json")
    val pub = if (body == null) HttpRequest.BodyPublishers.noBody() else HttpRequest.BodyPublishers.ofString(body)
    val resp = http.send(b.method(method, pub).build(), HttpResponse.BodyHandlers.ofString())
    // Only the status: response bodies echo catalog properties, which contain credentials.
    assert(resp.statusCode() / 100 == 2, s"$method $path -> HTTP ${resp.statusCode()}")
    resp.body()
  }

  private def createGravitinoIcebergCatalog(name: String): Unit = {
    call("POST", s"/api/metalakes/$metalake/catalogs",
      s"""{"name": "$name", "type": "RELATIONAL", "provider": "lakehouse-iceberg", "comment": "it",
         | "properties": ${propsTemplate.get.replace("__NAME__", name)}}""".stripMargin)
    created += name
  }

  override def beforeAll(): Unit = if (enabled) {
    createGravitinoIcebergCatalog(pre) // exists before Spark starts, so the plugin registers it
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("GravitinoSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.plugins", GravitinoSupport.PluginClass)
      .config("spark.sql.gravitino.uri", serverUri.get)
      .config("spark.sql.gravitino.metalake", metalake)
      .config("spark.sql.gravitino.enableIcebergSupport", "true")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
          "io.subhodeep.sparkext.CatalogExtension")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (enabled) {
    if (spark != null) {
      Seq(s"DROP TABLE IF EXISTS $late.db.t PURGE", s"DROP NAMESPACE IF EXISTS $late.db")
        .foreach(q => Try(sql(q).collect()))
      spark.stop()
    }
    created.foreach(c => Try(call("DELETE", s"/api/metalakes/$metalake/catalogs/$c?force=true")))
  }

  private def gated(name: String)(body: => Unit): Unit =
    test(name) { assume(enabled, "GRAVITINO_IT_URI / GRAVITINO_IT_CATALOG_PROPS not set"); body }

  gated("G1. plugin keeps our parser outermost and its catalogs show in SHOW CATALOGS") {
    val exts = spark.sparkContext.getConf.get("spark.sql.extensions").split(",").toSeq
    assert(exts.last === classOf[CatalogExtension].getName, exts)
    assert(exts.contains("org.apache.gravitino.spark.connector.iceberg.extensions." +
      "GravitinoIcebergSparkSessionExtensions"), exts)
    assert(spark.sessionState.sqlParser.isInstanceOf[CatalogExtParser])
    assert(catalogs.get(pre) === Some(IcebergGravitino))
    assert(errorOf(s"CREATE CATALOG $pre USING GRAVITINO").contains("already exists"))
    sql(s"CREATE CATALOG IF NOT EXISTS $pre USING GRAVITINO")
  }

  gated("G2. attach a catalog created in Gravitino after startup, then read/write through it") {
    createGravitinoIcebergCatalog(late)
    assert(!catalogs.contains(late))
    sql(s"CREATE CATALOG $late USING GRAVITINO")
    assert(catalogs.get(late) === Some(IcebergGravitino))
    // Catalogs on one JDBC database must not see each other's tables (catalog-backend-name).
    assert(sql(s"SHOW NAMESPACES IN $late").count() === 0)
    sql(s"CREATE NAMESPACE $late.db")
    sql(s"CREATE TABLE $late.db.t (id INT, s STRING) USING iceberg")
    sql(s"INSERT INTO $late.db.t VALUES (1, 'a'), (2, 'b')")
    val rows = sql(s"SELECT id, s FROM $late.db.t ORDER BY id").collect()
      .map(r => (r.getInt(0), r.getString(1))).toSeq
    assert(rows === Seq(1 -> "a", 2 -> "b"))
  }

  gated("G3. explicit class must match the catalog's provider") {
    createGravitinoIcebergCatalog(explicit)
    assert(errorOf(s"CREATE CATALOG $explicit USING '$HiveGravitino'").contains("binds to"))
    assert(!catalogs.contains(explicit))
    sql(s"CREATE CATALOG $explicit USING '$IcebergGravitino'")
    assert(catalogs.get(explicit) === Some(IcebergGravitino))
  }

  gated("G4. unknown Gravitino catalog fails at CREATE time") {
    val msg = errorOf(s"CREATE CATALOG it_missing_$run USING GRAVITINO")
    assert(msg.contains("does not exist in Gravitino metalake"), msg)
    assert(!catalogs.contains(s"it_missing_$run"))
  }

  gated("G5. DROP and re-attach a Gravitino catalog") {
    sql(s"DROP CATALOG $late")
    assert(!catalogs.contains(late))
    assert(errorOf(s"SET CATALOG $late").contains("not found"))
    sql(s"CREATE CATALOG $late USING GRAVITINO")
    assert(count(s"$late.db.t") === 2)
  }

  gated("G6. plain Iceberg SparkCatalog on Gravitino's Iceberg REST service sees the same data") {
    sql(s"""CREATE CATALOG $irc USING 'org.apache.iceberg.spark.SparkCatalog' OPTIONS (
           |  type 'rest', uri '$ircUri', warehouse '$late',
           |  header.X-Iceberg-Access-Delegation 'vended-credentials')""".stripMargin)
    assert(count(s"$irc.db.t") === 2)
    // Metadata tables only work on the plain Iceberg catalog; Gravitino's connector rejects them.
    val files = sql(s"SELECT file_path FROM $irc.db.t.files").collect().map(_.getString(0))
    assert(files.nonEmpty && files.forall(_.startsWith(s"s3://warehouse/$late/")), files.toSeq)
    sql(s"DROP CATALOG $irc")
  }
}
