package io.subhodeep.sparkext

import java.io.{PrintWriter, StringWriter}
import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._
import scala.reflect.io.Directory

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.parser.ParseException

class CatalogExtSuite extends AnyFunSuite with BeforeAndAfterAll {
  private val IcebergCatalog = "org.apache.iceberg.spark.SparkCatalog"
  private val Secret = "s3cr3t-Value-42" // value of CATALOGEXT_TEST_SECRET, set in build.sbt
  private val LiteralSecret = "literal-pw-123"

  private var tmp: Path = _
  private var spark: SparkSession = _

  private def sql(s: String): DataFrame = spark.sql(s)
  private def errorOf(s: String): Throwable = intercept[Exception](sql(s).collect())
  private def wh(name: String): String = tmp.resolve(s"wh-$name").toString
  private def createIceberg(name: String, extra: String = ""): Unit =
    sql(s"CREATE CATALOG $name USING '$IcebergCatalog' OPTIONS (type 'hadoop', warehouse '${wh(name)}'$extra)")
  private def catalogKeys(name: String): Set[String] =
    spark.conf.getAll.keys.filter(k => k == s"spark.sql.catalog.$name" ||
      k.startsWith(s"spark.sql.catalog.$name.")).toSet
  private def fullText(t: Throwable): String = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  override def beforeAll(): Unit = {
    tmp = Files.createTempDirectory("catalogext")
    val boot = tmp.resolve("bootstrap.json")
    Files.writeString(boot,
      s"""[{"name": "boot", "impl": "$IcebergCatalog",
         |  "options": {"type": "hadoop", "warehouse": "${wh("boot")}"}}]""".stripMargin)
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("CatalogExtSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.warehouse.dir", tmp.resolve("spark-warehouse").toString)
      // Test 8: Iceberg's parser and a dummy parser are both wrapped by ours, in that order.
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
          "io.subhodeep.sparkext.DummyParserExtension," +
          "io.subhodeep.sparkext.CatalogExtension")
      .config(Bootstrap.ConfKey, boot.toString)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    if (tmp != null) new Directory(tmp.toFile).deleteRecursively()
  }

  test("1. CREATE CATALOG, USE it, create table, insert, select") {
    createIceberg("lake")
    sql("USE lake")
    sql("CREATE NAMESPACE IF NOT EXISTS db")
    sql("CREATE TABLE db.t (id INT, s STRING) USING iceberg")
    sql("INSERT INTO db.t VALUES (1, 'a'), (2, 'b')")
    val rows = sql("SELECT id, s FROM lake.db.t ORDER BY id").collect()
      .map(r => (r.getInt(0), r.getString(1))).toSeq
    assert(rows === Seq(1 -> "a", 2 -> "b"))
    spark.catalog.setCurrentCatalog("spark_catalog")
  }

  test("2. IF NOT EXISTS is a no-op the second time; without it, duplicate fails") {
    createIceberg("dup")
    sql(s"CREATE CATALOG IF NOT EXISTS dup USING '$IcebergCatalog' OPTIONS (warehouse '/elsewhere')")
    assert(spark.conf.get("spark.sql.catalog.dup.warehouse") === wh("dup"))
    val e = errorOf(s"CREATE CATALOG dup USING '$IcebergCatalog'")
    assert(e.getMessage.contains("already exists"))
  }

  test("3. SHOW CATALOGS lists spark_catalog and created catalogs with class names") {
    val df = sql("SHOW CATALOGS")
    assert(df.columns.toSeq === Seq("catalog", "class"))
    val rows = df.collect().map(r => r.getString(0) -> r.getString(1)).toSeq
    assert(rows.map(_._1) === rows.map(_._1).sorted)
    val m = rows.toMap
    assert(m("spark_catalog") === CatalogCommands.SessionCatalogDefaultClass)
    assert(m("lake") === IcebergCatalog)
    assert(m("dup") === IcebergCatalog)
    assert(!m.keySet.exists(_.contains(".")))
  }

  test("4. DROP CATALOG removes conf and cached instance; guards reserved/current/missing") {
    createIceberg("dropme")
    sql("CREATE NAMESPACE dropme.ns") // forces the plugin to load and be cached
    assert(CatalogManagerAccess.isCached(spark, "dropme"))

    sql("DROP CATALOG dropme")
    assert(catalogKeys("dropme").isEmpty)
    assert(!CatalogManagerAccess.isCached(spark, "dropme"))
    val nf = errorOf("SET CATALOG dropme")
    assert(nf.getClass.getSimpleName === "CatalogNotFoundException")
    assert(nf.getMessage.contains("not found"))

    // Re-creating with different options takes effect, because the old instance was evicted.
    val w2 = tmp.resolve("wh-dropme-2")
    sql(s"CREATE CATALOG dropme USING '$IcebergCatalog' OPTIONS (type 'hadoop', warehouse '$w2')")
    sql("CREATE NAMESPACE dropme.ns2")
    assert(Files.isDirectory(w2.resolve("ns2")))

    Seq("spark_catalog", "SPARK_CATALOG").foreach { n =>
      assert(errorOf(s"DROP CATALOG $n").getMessage.contains("reserved"))
    }

    createIceberg("cur")
    sql("USE cur")
    assert(errorOf("DROP CATALOG cur").getMessage.contains("current catalog"))
    spark.catalog.setCurrentCatalog("spark_catalog")
    sql("DROP CATALOG cur;")
    assert(catalogKeys("cur").isEmpty)

    assert(errorOf("DROP CATALOG never_existed").getMessage.contains("not found"))
    sql("DROP CATALOG IF EXISTS never_existed")
  }

  test("4b. catalog name validation") {
    Seq("9bad", "a.b", "a-b").foreach { n =>
      assert(errorOf(s"CREATE CATALOG $n USING '$IcebergCatalog'").getMessage
        .contains("Invalid catalog name"), n)
    }
    assert(errorOf(s"CREATE CATALOG spark_catalog USING '$IcebergCatalog'").getMessage
      .contains("reserved"))
  }

  test("5. classes outside the allowlist are rejected, and SET cannot widen the allowlist") {
    val e = errorOf("CREATE CATALOG evil USING 'com.example.EvilCatalog'")
    assert(e.getMessage.contains("not allowed"))
    assert(catalogKeys("evil").isEmpty)

    spark.conf.set(Allowlist.ConfKey, "com.example.EvilCatalog")
    try {
      assert(errorOf("CREATE CATALOG evil USING 'com.example.EvilCatalog'").getMessage
        .contains("not allowed"))
    } finally spark.conf.unset(Allowlist.ConfKey)
  }

  test("6. ${env:X} substitution; unset vars fail; secrets never in logs or errors") {
    val createEnv =
      s"""CREATE CATALOG envcat USING '$IcebergCatalog' OPTIONS (
         |  type '$${env:CATALOGEXT_TEST_PLAIN}',
         |  warehouse '${wh("envcat")}',
         |  token 'Bearer $${env:CATALOGEXT_TEST_SECRET}',
         |  'rest.password' '$LiteralSecret')""".stripMargin

    val (errors, logs) = withLogCapture {
      sql(createEnv)
      sql("CREATE NAMESPACE envcat.ns") // the resolved 'type' really reached the catalog
      Seq(
        errorOf(createEnv), // already exists
        errorOf(s"CREATE CATALOG envmiss USING '$IcebergCatalog' OPTIONS (" +
          s"token '$LiteralSecret', type '$${env:CATALOGEXT_TEST_DEFINITELY_UNSET}')"),
        errorOf(s"CREATE CATALOG envbad USING 'x.Evil' OPTIONS (token '$${env:CATALOGEXT_TEST_SECRET}')"))
    }

    assert(spark.conf.get("spark.sql.catalog.envcat.type") === "hadoop")
    assert(spark.conf.get("spark.sql.catalog.envcat.token") === s"Bearer $Secret")

    val missing = errors(1).getMessage
    assert(missing.contains("CATALOGEXT_TEST_DEFINITELY_UNSET") && missing.contains("not set"))
    assert(catalogKeys("envmiss").isEmpty, "failed CREATE must leave no partial conf")

    assert(logs.contains("Registered catalog 'envcat'"), "log capture is not working")
    assert(logs.contains(Redact.Mask))
    (logs +: errors.map(fullText)).foreach { text =>
      assert(!text.contains(Secret))
      assert(!text.contains(LiteralSecret))
    }

    // Plan strings (EXPLAIN, UI, event log) are redacted as well.
    val plan = CatalogSqlParser.parse(createEnv).get.toString
    assert(!plan.contains(LiteralSecret) && plan.contains(Redact.Mask))
  }

  test("6b. EnvSubstitution unit cases") {
    val env = Map("A" -> "1", "B" -> "$x\\y").get _
    assert(EnvSubstitution.resolve("k", "${env:A}-${env:B}-${env:A}", env) === "1-$x\\y-1")
    assert(EnvSubstitution.resolve("k", "no refs", env) === "no refs")
    assert(intercept[CatalogExtException](EnvSubstitution.resolve("k", "${env:1x}", env))
      .getMessage.contains("invalid environment variable name"))
  }

  test("7. normal SQL passes through unchanged") {
    assert(sql("SELECT 1").collect().head.getInt(0) === 1)
    sql("CREATE TABLE spark_catalog.default.pt (id INT) USING parquet")
    val tables = sql("SHOW TABLES IN spark_catalog.default").collect().map(_.getString(1))
    assert(tables.contains("pt"))
    // SHOW CATALOGS with a pattern is Spark's native command.
    assert(sql("SHOW CATALOGS LIKE 'la*'").columns.toSeq === Seq("catalog"))
    intercept[ParseException](sql("SELEC 1"))
    intercept[ParseException](sql("CREATE CATALOGUE x"))
  }

  test("8. coexists with other parser extensions (dummy + Iceberg)") {
    assert(spark.sessionState.sqlParser.isInstanceOf[CatalogExtParser])
    assert(sql("PING DUMMY").collect().head.getString(0) === "pong")
    sql("ALTER TABLE lake.db.t WRITE ORDERED BY id") // Iceberg extension grammar
    assert(sql("SELECT count(*) FROM lake.db.t").collect().head.getLong(0) === 2)
  }

  test("9. case, whitespace, quoting and comment edge cases") {
    import CatalogSqlParser.parse
    assert(parse("create   catalog  x  using 'c' options (a 'it''s')") ===
      Some(CreateCatalogCmd("x", "c", Seq("a" -> "it's"), ifNotExists = false)))
    assert(errorOf("create   catalog  x  using 'c' options (a 'it''s')").getMessage
      .contains("not allowed"))

    val multi =
      """-- leading comment
        |Create Catalog
        |  If Not Exists   ml_1
        |  USING 'org.apache.iceberg.spark.SparkCatalog'
        |  OPTIONS (
        |    'type' = 'hadoop',
        |    warehouse '/tmp/x', /* inline */
        |    header.X-Iceberg-Access-Delegation 'vended-credentials',
        |    empty ''
        |  ) ;;""".stripMargin
    assert(parse(multi) === Some(CreateCatalogCmd("ml_1", IcebergCatalog, Seq(
      "type" -> "hadoop", "warehouse" -> "/tmp/x",
      "header.X-Iceberg-Access-Delegation" -> "vended-credentials", "empty" -> ""),
      ifNotExists = true)))
    assert(parse("CREATE CATALOG c USING 'k' OPTIONS ()") ===
      Some(CreateCatalogCmd("c", "k", Nil, ifNotExists = false)))
    assert(parse("create catalog if using 'k'") ===
      Some(CreateCatalogCmd("if", "k", Nil, ifNotExists = false)))
    assert(parse("drop catalog if exists x;") === Some(DropCatalogCmd("x", ifExists = true)))
    assert(parse("DROP\n CATALOG x") === Some(DropCatalogCmd("x", ifExists = false)))
    assert(parse("  show CATALOGS ; ") === Some(ShowCatalogsCmd()))
    Seq("show catalogs like 'x'", "SELECT 1", "create catalogue x", "", "  ", "show")
      .foreach(s => assert(parse(s).isEmpty, s))

    def syntaxError(s: String): String = intercept[CatalogExtException](parse(s)).getMessage
    assert(syntaxError("CREATE CATALOG `bad` USING 'k'").contains("unexpected character '`'"))
    assert(syntaxError("CREATE CATALOG x 'k'").contains("expected USING"))
    assert(syntaxError("CREATE CATALOG x USING k").contains("single-quoted"))
    assert(syntaxError("CREATE CATALOG x USING 'k' OPTIONS (a b)").contains("single-quoted"))
    assert(syntaxError("CREATE CATALOG x USING 'k' extra").contains("expected end of statement"))
    assert(syntaxError("DROP CATALOG").contains("expected a catalog name"))
    val unterminated = syntaxError(s"CREATE CATALOG x USING 'k' OPTIONS (token '$LiteralSecret")
    assert(unterminated.contains("unterminated string") && !unterminated.contains(LiteralSecret))
    assert(!syntaxError(s"CREATE CATALOG x USING 'k' OPTIONS (token '$LiteralSecret' junk)")
      .contains(LiteralSecret))
  }

  test("10. phase 2: bootstrap file registers catalogs on first statement") {
    assert(spark.conf.get("spark.sql.catalog.boot") === IcebergCatalog)
    sql("CREATE NAMESPACE boot.ns")
    assert(Files.isDirectory(tmp.resolve("wh-boot").resolve("ns")))
  }

  test("10b. bootstrap file validation never echoes values") {
    def bad(json: String): String = {
      val f = Files.createTempFile(tmp, "boot", ".json")
      Files.writeString(f, json)
      intercept[CatalogExtException](Bootstrap.readEntries(f)).getMessage
    }
    assert(bad(s"""[{"name": "x", "impl": "k", "options": {"token": $LiteralSecret}}]""")
      .contains("malformed JSON"))
    assert(!bad(s"""[{"name": "x", "impl": "k", "options": {"token": $LiteralSecret}}]""")
      .contains(LiteralSecret))
    assert(bad("""{"name": "x"}""").contains("JSON array"))
    assert(bad("""[{"name": "x", "impl": "k", "option": {}}]""").contains("unknown field 'option'"))
    assert(bad("""[{"name": "x", "impl": "k", "options": {"n": 1}}]""").contains("must be a string"))
    assert(bad("""[{"impl": "k"}]""").contains("'name' must be a string"))
    val ok = Files.createTempFile(tmp, "boot", ".json")
    Files.writeString(ok, """[{"name": "a", "impl": "k"}, {"name": "b", "impl": "k", "options": {"x": "1"}}]""")
    assert(Bootstrap.readEntries(ok) === Seq(
      CreateCatalogCmd("a", "k", Nil, ifNotExists = true),
      CreateCatalogCmd("b", "k", Seq("x" -> "1"), ifNotExists = true)))
  }

  test("11. Gravitino: USING GRAVITINO parses; without the plugin it fails clearly") {
    assert(CatalogSqlParser.parse("create catalog g using gravitino;") ===
      Some(CreateCatalogCmd("g", GravitinoSupport.Keyword, Nil, ifNotExists = false)))
    assert(CatalogSqlParser.parse("CREATE CATALOG g USING Gravitino OPTIONS (a '1')") ===
      Some(CreateCatalogCmd("g", GravitinoSupport.Keyword, Seq("a" -> "1"), ifNotExists = false)))
    assert(!GravitinoSupport.pluginEnabled(spark))
    assert(errorOf("CREATE CATALOG g USING GRAVITINO").getMessage.contains("not in spark.plugins"))
    // Not on the default allowlist unless the plugin is enabled.
    assert(errorOf(s"CREATE CATALOG g USING '${GravitinoSupport.CatalogClasses.head}'").getMessage
      .contains("not allowed"))
    assert(catalogKeys("g").isEmpty)
  }

  /** Captures everything logged (all loggers that reach root) while `f` runs. */
  private def withLogCapture[T](f: => T): (T, String) = {
    val lines = new ConcurrentLinkedQueue[String]()
    val appender = new AbstractAppender("catalogext-capture", null, null, true,
        Property.EMPTY_ARRAY) {
      override def append(e: LogEvent): Unit = {
        lines.add(e.getMessage.getFormattedMessage)
        Option(e.getThrown).foreach(t => lines.add(fullText(t)))
      }
    }
    val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val root = ctx.getConfiguration.getRootLogger
    appender.start()
    root.addAppender(appender, Level.ALL, null)
    ctx.updateLoggers()
    try {
      val r = f
      (r, lines.asScala.mkString("\n"))
    } finally {
      root.removeAppender(appender.getName)
      ctx.updateLoggers()
      appender.stop()
    }
  }
}
