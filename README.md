# spark-catalog-ext

A thin jar that adds `CREATE CATALOG`, `DROP CATALOG` and `SHOW CATALOGS` to Spark SQL without
modifying Spark. It loads through `spark.sql.extensions` and only wraps the SQL parser: it injects
no analyzer, optimizer or planner rules, so it does not interact with Iceberg's rules or Comet.

- Spark 3.5.8, Scala 2.13.8, Java 17 (bytecode target 61)
- Spark, Iceberg, Jackson and the Gravitino connector are `Provided`; the jar contains only
  `io.subhodeep.sparkext` classes (~90 KB)
- Optional integration with Apache Gravitino 1.3.x: `CREATE CATALOG x USING GRAVITINO` attaches a
  catalog from a Gravitino metalake at runtime

Contents: [Build](#build) · [SQL syntax](#sql-syntax) · [Configuration](#configuration) ·
[Usage](#usage) · [Apache Gravitino 1.3.x](#apache-gravitino-13x) ·
[Security model](#security-model) · [Bootstrap file](#phase-2-bootstrap-file) ·
[DROP limitation](#known-limitation-drop-uses-reflection) · [Compatibility](#compatibility) ·
[Design decisions](#design-decisions-and-deviations) · [Tests](#tests) ·
[Project layout](#project-layout)

## Build

```bash
sbt test       # 14 tests, local SparkSession + Iceberg Hadoop catalog (+6 Gravitino ITs, skipped without a server)
sbt package    # -> target/scala-2.13/spark-catalog-ext_2.13-0.1.0.jar
```

No `sbt` on the PATH? The Spark tree ships a launcher:
`java -jar ~/Documents/spark-3.5.8/build/sbt-launch-1.9.2.jar test package` (it fetches sbt 1.9.9 per
`project/build.properties`).

## SQL syntax

```sql
CREATE CATALOG [IF NOT EXISTS] <name> USING '<fully.qualified.CatalogClass>' | GRAVITINO
  [OPTIONS (key 'value', key2 'value2')];
DROP CATALOG [IF EXISTS] <name>;
SHOW CATALOGS;
```

`USING GRAVITINO` attaches a catalog defined in Apache Gravitino; see
[Apache Gravitino 1.3.x](#apache-gravitino-13x).

Example (Iceberg REST on Polaris):

```sql
CREATE CATALOG IF NOT EXISTS lake USING 'org.apache.iceberg.spark.SparkCatalog' OPTIONS (
  type 'rest',
  uri 'https://polaris.example.com/api/catalog',
  warehouse 'analytics',
  scope 'PRINCIPAL_ROLE:ALL',
  header.X-Iceberg-Access-Delegation 'vended-credentials',
  credential '${env:POLARIS_CREDENTIAL}'
);
USE lake;
```

Parsing rules:

- Keywords are case-insensitive. Whitespace, newlines, `-- line` and `/* block */` comments are allowed.
  Trailing `;` (one or more) is optional.
- `name` is a bare identifier matching `^[A-Za-z_][A-Za-z0-9_]*$`. Catalog names are case-sensitive,
  as in Spark (`lake` and `LAKE` are different catalogs). `spark_catalog` is reserved, in any case.
- `USING` takes a single-quoted class name.
- Option keys are bare (`header.X-Iceberg-Access-Delegation`, `s3.endpoint`) or single-quoted. The
  `=` between key and value is optional. Values must be single-quoted. `''` is an escaped quote;
  backslashes are literal. Duplicate keys (case-insensitive) are rejected.
- A statement that starts with `CREATE CATALOG` or `DROP CATALOG` is handled by this extension, so a
  syntax error in it gets a specific message. Anything else, including `SHOW CATALOGS LIKE '...'`,
  goes to the next parser unchanged.

Semantics:

| Statement | Effect |
|---|---|
| `CREATE CATALOG` | Validates the name, the allowlist and the option keys, resolves `${env:…}`, then sets `spark.sql.catalog.<name>` = class and `spark.sql.catalog.<name>.<key>` = value in the **session** conf. It does not load the plugin; Spark loads and caches it on first use. Fails if `spark.sql.catalog.<name>` is already set, and does nothing if `IF NOT EXISTS` is given. |
| `DROP CATALOG` | Refuses `spark_catalog` and the current catalog. Unsets `spark.sql.catalog.<name>` and `spark.sql.catalog.<name>.*`, then evicts the cached plugin from the session's `CatalogManager`. A later `USE`/`SET CATALOG <name>` fails with `CatalogNotFoundException`. |
| `SHOW CATALOGS` | Columns `catalog`, `class`. Lists every session-conf key that is exactly `spark.sql.catalog.<name>`, plus `spark_catalog` (shown as `V2SessionCatalog` unless overridden), sorted by name. |

All three are session-scoped, because session conf is. Catalogs set with `--conf` come back in every
new session even after a `DROP` in one session.

## Configuration

| Key | Scope | Default | Meaning |
|---|---|---|---|
| `spark.sql.extensions` | static | — | Add `io.subhodeep.sparkext.CatalogExtension` (last, see below). |
| `spark.sql.catalogext.allowedClasses` | **static only** | `org.apache.iceberg.spark.SparkCatalog,org.apache.iceberg.spark.SparkSessionCatalog`, plus Gravitino's built-in Spark 3.5 catalog classes when `GravitinoSparkPlugin` is in `spark.plugins` | Comma-separated catalog classes that `CREATE CATALOG` may register. An explicit value replaces the whole default, including the Gravitino classes. |
| `spark.sql.catalogext.bootstrapFile` | **static only** | unset | Optional phase 2: local JSON file of catalogs to register (see below). |

"Static only" means the value is read from the application's `SparkConf` (`--conf`,
`spark-defaults.conf`). A runtime `SET` is ignored, so SQL users cannot widen the allowlist.

## Usage

Order matters only among extensions that also wrap the parser. Put ours last so it sees statements
first and passes everything else down to the Iceberg parser:

```
--conf spark.sql.extensions=<iceberg-ext>,<comet-ext>,io.subhodeep.sparkext.CatalogExtension
--jars spark-catalog-ext_2.13-<version>.jar
```

### spark-submit

```bash
export POLARIS_CREDENTIAL='client-id:client-secret'
spark-submit \
  --jars /opt/jars/spark-catalog-ext_2.13-0.1.0.jar,/opt/jars/iceberg-spark-runtime-3.5_2.13-1.10.1.jar,/opt/jars/comet-spark-spark3.5_2.13-0.17.0-SNAPSHOT.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.comet.CometSparkSessionExtensions,io.subhodeep.sparkext.CatalogExtension \
  --conf spark.redaction.regex='(?i)secret|password|token|access[.]?key|credential' \
  --conf spark.sql.catalogext.allowedClasses=org.apache.iceberg.spark.SparkCatalog \
  my_job.py
```

### Kubernetes

```bash
spark-submit \
  --master k8s://https://<api-server>:6443 --deploy-mode cluster \
  --conf spark.kubernetes.container.image=<registry>/spark:3.5.8-custom \
  --conf spark.jars=local:///opt/spark/extra-jars/spark-catalog-ext_2.13-0.1.0.jar,local:///opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.13-1.10.1.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.comet.CometSparkSessionExtensions,io.subhodeep.sparkext.CatalogExtension \
  --conf spark.redaction.regex='(?i)secret|password|token|access[.]?key|credential' \
  --conf spark.kubernetes.driver.secretKeyRef.POLARIS_CREDENTIAL=polaris-creds:credential \
  local:///opt/spark/app/my_job.py
```

`local://` means the jar is baked into the image. Use `s3a://…/spark-catalog-ext_2.13-0.1.0.jar`
instead to fetch it at submit time. `${env:…}` is resolved on the **driver**, where the commands run,
so only the driver needs the variable (`secretKeyRef` above). To use a bootstrap file, mount it from
a ConfigMap with a driver pod template and point `spark.sql.catalogext.bootstrapFile` at the
mount path.

## Apache Gravitino 1.3.x

Works with `gravitino-spark-connector-runtime-3.5_2.13` **1.3.0 and 1.3.1**. The jar is compiled
against 1.3.1 as `Provided`, and none of it is bundled. Without the Gravitino jar on the classpath
everything else keeps working: only the code behind `USING GRAVITINO` and Gravitino classes links
against Gravitino.

How the pieces fit:

- `GravitinoSparkPlugin` (in `spark.plugins`) registers one Spark catalog per relational catalog of
  the metalake **at startup**. It writes `spark.sql.catalog.<name>=Gravitino…CatalogSpark35` into
  the static conf with no options. These catalogs show in `SHOW CATALOGS`, and `CREATE CATALOG` on
  one of them fails with "already exists" (no-op with `IF NOT EXISTS`).
- Gravitino's catalog classes fetch their properties from the Gravitino server **by name** when
  Spark loads them. This extension makes that useful at runtime: a catalog created in Gravitino
  after the application started can be attached without a restart:

  ```sql
  CREATE CATALOG sales USING GRAVITINO;   -- class picked from the catalog's provider
  -- or explicitly:
  CREATE CATALOG sales USING 'org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark35';
  ```

- Before writing any conf, `CREATE CATALOG` looks the catalog up in Gravitino. This is a metadata
  call through the plugin's own client; the Spark catalog is still loaded lazily. It fails early
  and clearly when:
  - the plugin is not in `spark.plugins`
  - the catalog does not exist in the metalake
  - the catalog is not relational
  - the provider has no Spark binding
  - an explicit class does not match the provider (e.g. the Hive class for a `lakehouse-iceberg`
    catalog)
  - the provider is `lakehouse-iceberg`/`lakehouse-paimon` without
    `spark.sql.gravitino.enableIcebergSupport`/`enablePaimonSupport=true`. This mirrors the
    plugin, which skips those catalogs too.
- `USING GRAVITINO` **attaches** a catalog that already exists in the metalake. It never creates
  one in Gravitino; that stays an admin operation (Gravitino UI/API).
- **Lookup.** On a name the plugin hasn't cached, the jar first refreshes the plugin's catalog
  cache from the server's catalog *list* (`loadRelationalCatalogs()`), and loads the catalog only
  if it is listed. Gravitino's `getGravitinoCatalogInfo` logs a full ERROR stack trace for every
  missing name, so this keeps a typo down to the one-line `[CATALOGEXT]` error. A side effect: the
  refresh also updates the cached metadata of the metalake's other catalogs.
- `OPTIONS` still apply. Gravitino merges Spark options into the catalog properties from the
  server (`toSparkCatalogProperties(options, properties)`).
- **Allowlist.** If `spark.sql.catalogext.allowedClasses` is unset and the plugin is enabled,
  Gravitino's built-in classes are allowed automatically. Enabling the plugin is already an
  explicit admin opt-in, and the catalog definitions come from the Gravitino server, not from the
  SQL user. With `USING GRAVITINO` the resolved class is still checked against the allowlist, so a
  class supplied by a custom `SparkCatalogExtension` provider has to be listed explicitly.
- **Extension order.** The plugin rewrites `spark.sql.extensions` to
  `<Gravitino extensions>, <your list>` (deduplicated, Gravitino's first). Ours stays last, so it
  still sees statements first. `GravitinoIcebergSparkSessionExtensions` adds only a planner
  strategy, not a parser, so nothing conflicts.
- **DROP** of a Gravitino-backed catalog works like any other: it is per-session and removes
  nothing from Gravitino. The plugin's `GravitinoCatalogManager` caches catalog *metadata* with no
  expiry. If a catalog is dropped and recreated **in Gravitino** with different properties, the
  Spark application keeps the old metadata until it restarts. This is Gravitino behaviour that
  `DROP CATALOG` cannot clear.
- **One metalake per Spark application.** `GravitinoSparkPlugin` creates a single
  `GravitinoCatalogManager` for `spark.sql.gravitino.metalake` at startup and refuses a second
  one. Every Gravitino catalog class, and therefore `USING GRAVITINO`, resolves through it, so a
  session cannot switch metalakes; start another application with a different
  `spark.sql.gravitino.metalake`. Iceberg catalogs of another metalake can still be used in the
  same session through an Iceberg REST service bound to that metalake, as plain
  `org.apache.iceberg.spark.SparkCatalog` catalogs (`type 'rest'`).
- Spark SQL cannot use a Gravitino catalog whose name falls outside
  `^[A-Za-z_][A-Za-z0-9_]*$` (e.g. one with a hyphen) through this grammar.
- **Paimon.** `CatalogNameAdaptor` maps `lakehouse-paimon` to `GravitinoPaimonCatalogSpark35`, but
  neither the 1.3.0 nor the 1.3.1 `…runtime-3.5_2.13` jar contains that class. Paimon catalogs
  through Gravitino will not load on Scala 2.13, with or without this extension.

Gravitino 1.3.1 deployment quirks (not caused by this jar; found in end-to-end testing):

- **JDBC driver on the Spark side.** For a `catalog-backend=jdbc` catalog, the Spark connector calls
  `Class.forName()` on the catalog's `jdbc-driver` on the driver, even when it auto-routes the
  catalog through Iceberg REST. Put the JDBC driver (e.g. `postgresql-42.7.11.jar`) on Spark's
  classpath.
- **`iceberg-aws-bundle` placement.** It must sit beside `iceberg-spark-runtime`. With the runtime
  in `$SPARK_HOME/jars`, add the bundle via `spark.driver/executor.extraClassPath` (or
  `$SPARK_HOME/jars`), not `--jars`. Otherwise S3FileIO fails with
  `Cannot find constructor for interface org.apache.iceberg.io.FileIO`.
- **Iceberg metadata tables** (`db.t.files`, `.snapshots`) are rejected through Gravitino catalogs.
  Query them through a plain Iceberg REST catalog on Gravitino's service.
- **Give every JDBC-backed catalog its own `catalog-backend-name`.** It is the Iceberg
  `JdbcCatalog` name the metadata rows are keyed by, and Gravitino defaults it to the backend type
  (`jdbc`). Two Gravitino catalogs on the same database without it silently share all schemas
  and tables (only the S3 paths differ). `DROP NAMESPACE` in one then acts on the other's data.
  Found in testing; it can be changed later with a `setProperty` catalog update plus moving the
  `catalog_name` rows.
- **S3 catalogs need `credential-providers`.** Routing through Iceberg REST is refused for an S3
  warehouse without it, because vended credentials replace static ones.
- **Comet leaks vended keys.** Comet's `CometIcebergNativeScan` writes the vended S3 keys into the
  event log's plan description; see Security model.

Config (Gravitino keys are the connector's own). This is the set verified on `/opt/spark` with
Comet, a JDBC-backed catalog and an S3 warehouse; `iceberg-spark-runtime` is in
`$SPARK_HOME/jars`:

```bash
spark-submit \
  --jars /opt/jars/spark-catalog-ext_2.13-0.1.0.jar,/opt/jars/gravitino-spark-connector-runtime-3.5_2.13-1.3.1.jar,/opt/jars/postgresql-42.7.11.jar \
  --conf spark.driver.extraClassPath=/opt/jars/iceberg-aws-bundle-1.10.1.jar \
  --conf spark.executor.extraClassPath=/opt/jars/iceberg-aws-bundle-1.10.1.jar \
  --conf spark.plugins=org.apache.spark.CometPlugin,org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin \
  --conf spark.sql.gravitino.uri=http://gravitino:8090 \
  --conf spark.sql.gravitino.metalake=prod \
  --conf spark.sql.gravitino.enableIcebergSupport=true \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.comet.CometSparkSessionExtensions,io.subhodeep.sparkext.CatalogExtension \
  --conf "spark.redaction.regex=(?i)secret|password|token|access[.]?key|credential" \
  my_job.py
```

`--conf spark.plugins=…` replaces the value from `spark-defaults.conf`, so list every plugin you
already use (here Comet's).

### External-style Iceberg tables (like Unity Catalog external tables)

Verified through a Gravitino `lakehouse-iceberg` catalog on S3:

```sql
-- Table data and metadata at a path you choose
CREATE TABLE lakehouse.ext.events (id BIGINT, kind STRING)
  USING iceberg LOCATION 's3://warehouse/external/events';

DROP TABLE lakehouse.ext.events;            -- unregisters only; files stay in S3
DROP TABLE lakehouse.ext.events PURGE;      -- unregisters and deletes the files

-- (Re)attach existing Iceberg data from its metadata file
CALL lakehouse.system.register_table(
  table => 'ext.events',
  metadata_file => 's3://warehouse/external/events/metadata/00001-<uuid>.metadata.json');
```

- Iceberg has no managed/external flag, so `DESCRIBE` shows `Type: MANAGED` regardless. Whether
  files survive depends only on `DROP` versus `DROP … PURGE`.
- The re-registered table is visible through Gravitino's API and UI, with its `location`.
- To find the current metadata file, query `<table>.metadata_log_entries` through a plain Iceberg
  REST catalog. Metadata tables are rejected through Gravitino catalogs.
- The location must be reachable with the credentials the catalog vends.
- Non-Iceberg external tables (Parquet/CSV/Delta files) need a Gravitino Hive catalog backed by a
  Hive Metastore. That path has not been tested.

Gravitino's **Iceberg REST service** (port 9001 by default) needs nothing Gravitino-specific. It is
a standard Iceberg REST catalog:

```sql
CREATE CATALOG irc USING 'org.apache.iceberg.spark.SparkCatalog'
  OPTIONS (type 'rest', uri 'http://gravitino:9001/iceberg/');
```

Gravitino 1.3.1 is built on Iceberg 1.11.0 (server libs); it is used here with Iceberg 1.10.1 on
the Spark side (production) and 1.12.0 (unit tests).

## Security model

1. **Allowlist.** `CREATE CATALOG` registers only the classes listed in
   `spark.sql.catalogext.allowedClasses`, which is read from the static SparkConf. Any other class
   fails with `[CATALOGEXT] Catalog class '…' is not allowed`.
2. **`${env:VAR_NAME}`** in option values is resolved from the driver's environment when the command
   runs, not when it is parsed. A value may contain several references (`'Bearer ${env:TOK}'`). If a
   variable is unset, the statement fails and no conf is written. Resolved values are never logged.
3. **Redaction.** Option values whose key contains `secret`, `token`, `password`, `credential` or
   `key` (case-insensitive) are masked in our log lines and in the command's plan string (EXPLAIN,
   UI, event log plan). Error messages never contain option values. Syntax and bootstrap-file
   errors never echo string literals.

**What this jar cannot protect (verified on the custom 3.5.8 build with event logging on):**

- **Runtime session conf reaches the event log unredacted.** `SQLExecution` copies every session conf
  into job/stage local properties. `EventLoggingListener` redacts those properties only for keys
  present in the **static** SparkConf. So any secret set at runtime, whether by `CREATE CATALOG`,
  `SET` or `spark.conf.set`, appears in clear text in `SparkListenerJobStart`/`StageSubmitted`
  `Properties`. The same secret passed as `--conf` does not. Mitigation: pass secret options
  statically (`--conf spark.sql.catalog.lake.credential=…`) and use `CREATE CATALOG lake … OPTIONS`
  only for the non-secret keys. This works because `CREATE CATALOG` checks only
  `spark.sql.catalog.<name>`, not sub-keys. Alternatively restrict access to the event-log
  directory. `CREATE CATALOG` logs a WARN (key names only) when a sensitive option is used while
  `spark.eventLog.enabled=true`.
- **Spark's redaction regex does not include `credential`.** The default `spark.redaction.regex` is
  `(?i)secret|password|token|access[.]?key`, so Polaris's `credential` shows in `SET` output and in
  `SQLExecutionStart.modifiedConfigs`. Add `credential` to the regex as in the examples above
  (verified to fix both).
- **The Comet fork's native Iceberg scan prints catalog properties.** `CometIcebergNativeScan`
  serializes `catalog_properties` (including `token`/`credential`) into `physicalPlanDescription` in
  the event log. This also covers **vended** credentials: with Gravitino credential vending,
  Comet 1.0.0-preview writes the vended `s3.access-key-id`/`s3.secret-access-key` there. Credential
  vending therefore does not keep keys out of event logs while Comet scans the table. Fix this in
  the Comet fork.
- **Plain `SET` bypasses the allowlist.** `SET spark.sql.catalog.x=any.Class` registers a catalog
  without going through this extension. The allowlist governs `CREATE CATALOG` only.
- **Spark itself substitutes `${env:…}` in all SQL text** (`spark.sql.variable.substitute=true` by
  default), so a SQL user can run `SELECT '${env:POLARIS_CREDENTIAL}'`. In multi-tenant
  deployments, set `spark.sql.variable.substitute=false`. Our `${env:}` resolution is independent
  of that flag.
- A secret written as a literal in SQL text shows up wherever Spark shows SQL text (UI job
  descriptions, Thrift server logs). Use `${env:…}`.

## Phase 2: bootstrap file

```json
[
  {"name": "lake", "impl": "org.apache.iceberg.spark.SparkCatalog",
   "options": {"type": "rest", "uri": "https://polaris/api/catalog", "credential": "${env:POLARIS_CREDENTIAL}"}}
]
```

- It is enabled only when `spark.sql.catalogext.bootstrapFile` is set; if unset, it is skipped silently.
- The file is local (`java.nio`; on Kubernetes, mount it), parsed with Spark's bundled Jackson.
- It loads once per session, on the first `parsePlan` call. Each entry runs through the same
  `CreateCatalogCmd` validation with **IF NOT EXISTS** semantics, so a catalog already defined via
  `--conf` wins.
- Parsing is strict: the top level must be an array, entries may only contain `name`/`impl`/`options`,
  and option values must be strings. Errors name the entry and field, never values (Jackson's own
  messages, which quote input, are replaced).
- If loading fails, the error is thrown and retried on the next statement. A broken file is never
  silently ignored.

## Known limitation: DROP uses reflection

Spark 3.5 has no public API to unload a catalog plugin, and `CatalogManager` is `private[sql]`.
`CatalogManagerAccess` (the only file touching internals) reads the private
`catalogs: mutable.HashMap[String, CatalogPlugin]` field reflectively and removes the entry while
holding the manager's own monitor. If a future build renames or retypes that field, `DROP CATALOG`
still removes the config but fails with:

> `[CATALOGEXT] Catalog 'x' config was removed, but its cached instance could not be evicted (…); it stays usable until the session ends.`

Also:

- The evicted plugin is not closed: `CatalogPlugin` has no close contract, and another thread may
  still hold it.
- Cached DataFrames or `CACHE TABLE` entries over its tables are not invalidated.
- `CREATE CATALOG` also evicts a stale cached instance on a best-effort basis (only possible after a
  failed eviction). It does not depend on reflection to succeed.

## Compatibility

Checked against the custom build at `~/Documents/spark-3.5.8`
(`Spark 3.5.8 (git revision 35c89fe80bd) built for Hadoop 3.3.4`, flags
`-Pkubernetes -Pscala-2.13`). `CUSTOM_SPARK_HOME` was unset, so `dist/` in that tree was used.

| Check | Result |
|---|---|
| Source diff vs. `v3.5.8-rc1` (`5a48a37b2db..HEAD`), `*.scala`/`*.java`/`*.g4` | **No changes.** Custom commits touch POMs (Scala 2.13 default), SQL UI static `spark-sql-viz.js/css`, and config files. |
| `javap -p -s` of `ParserInterface`, `DataTypeParserInterface`, `CatalogManager` | **Identical** to Maven Central 3.5.8 |
| `javap -p -s` of other Spark types used (`SparkSessionExtensions`, `RunnableCommand`, `LeafRunnableCommand`, `AnalysisException`, `SessionState`, `RuntimeConfig`, `catalog.Catalog`) | **Identical** |
| Byte comparison of the above `.class` files | **Byte-identical** |
| Whole-jar comparison, spark-catalyst / spark-sql / spark-sql-api | Same class counts (4225 / 2846 / 781). 234 / 107 / 397 classes differ in bytes, all with class-file version 52 in both builds. The custom build was compiled with JDK 17 (`Build-Jdk-Spec: 17`) and stock with JDK 8. The differing classes are Java sources (incl. ANTLR-generated `SqlBaseParser`) and a few Scala classes that call JDK 9+ covariant `ByteBuffer` overloads. That is compiler output, not API change. |
| `CatalogManager.catalogs` field (reflection target) | Present, `private final scala.collection.mutable.HashMap<String, CatalogPlugin>` in both |
| End-to-end on custom `dist/` with Iceberg 1.10.1 + Comet 0.17.0-SNAPSHOT (`spark-shell`, extensions in production order) | CREATE → SHOW → create table/insert/select (1000 rows) → Iceberg `WRITE ORDERED BY` → Comet native plan (`CometIcebergNativeScan`, `CometHashAggregate`, `CometNativeShuffle`) → DROP → `CatalogNotFoundException`: **all pass** |

**Conclusion:** binary compatible, and nothing needed adapting. One runtime note: the custom build's
JDK 17-compiled `ByteBuffer` call sites require Java 9+ at runtime, which you already run.

Unit tests use Iceberg **1.12.0** (latest 1.x with a Spark 3.5 runtime). Production uses
**1.10.1**, which the end-to-end run above covered.

### Gravitino 1.3.0 vs 1.3.1

The jar calls only these Gravitino APIs:

- `GravitinoCatalogManager.get()`
- `GravitinoCatalogManager.getCatalogs()`, `loadRelationalCatalogs()`,
  `getGravitinoCatalogInfo(String)`
- `Catalog.type()`, `Catalog.provider()`
- `CatalogNameAdaptor.getCatalogName(String)`

| Check (`javap -p`, runtime-3.5_2.13 jars) | Result |
|---|---|
| `CatalogNameAdaptor`, `Catalog`, `Catalog$Type`, `GravitinoSparkPlugin`, `GravitinoDriverPlugin`, `BaseCatalog` | **Identical** public API |
| `GravitinoCatalogManager` | Every method the jar calls is identical. 1.3.1 changes `create(...)` (called by the plugin, not us) and adds `getIcebergRestUri()` |
| Built-in Spark 3.5 catalog classes | Iceberg, Hive, Glue, JDBC and PostgreSQL present in both. **Paimon missing in both** |
| Custom Spark `dist/` **without** Gravitino on the classpath | Iceberg CREATE/insert/select works. `USING GRAVITINO` gives "plugin is not in spark.plugins", and a Gravitino class gives "not allowed". No `NoClassDefFoundError` |
| Against a live Gravitino 1.3.1 server (`GravitinoSuite` G1–G6, sbt, Iceberg 1.12.0) | **All pass** |
| `/opt/spark` (same custom 3.5.8 jars) + Comet 1.0.0-preview + Iceberg 1.10.1 against Gravitino 1.3.1: JDBC-backed catalog, S3 (SeaweedFS) warehouse, routed through Iceberg REST with vended credentials | **21/21 smoke checks pass.** Covers a plugin-registered catalog, `USING GRAVITINO` for a catalog created after startup, isolation between catalogs, Comet native scan, CREATE-time validation, DROP/re-attach, a plain Iceberg REST catalog reading the same table, and data files in S3. Also verified: Iceberg `LOCATION`, `DROP` with and without `PURGE`, and `register_table` |

## Design decisions and deviations

These were chosen where the spec was open, taking the simpler option:

- **`CatalogManager` is `private[sql]`.** The spec's reflection helper therefore holds it as
  `AnyRef` and looks up the class by name, instead of adding a bridge class into Spark's
  `org.apache.spark.sql` package. The current catalog comes from the public
  `spark.catalog.currentCatalog()`.
- **Errors** are `CatalogExtException extends AnalysisException` (prefixed `[CATALOGEXT]`), using
  the subclass-accessible constructor, with no fake Spark error class. Syntax errors use it too,
  rather than Spark's `ParseException`.
- **`SHOW CATALOGS` shadows Spark's native command.** The native one lists only *loaded* catalogs
  with one column. `SHOW CATALOGS LIKE '…'` still goes to the native command.
- **`IF` is a keyword only when followed by `NOT EXISTS` / `EXISTS`**, so `if` remains usable as a
  name.
- **Option values must be single-quoted** (no bare numbers or booleans). Backslash is not an escape.
- **Allowlist and bootstrap file are read from the static SparkConf** (security; see above).
- **Option keys are validated** against `^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$`.
- **Added:** a WARN on sensitive options with event logging on, and plan-string redaction via
  `stringArgs`.
- **Bytecode targets Java 17.** `-target:17` was added next to `-release 17`, because Scala 2.13.8's
  `-release` alone still emits Java 8 bytecode.
- **Gravitino support is an addition, not part of the original spec.** It adds the
  `USING GRAVITINO` keyword (a quoted `'GRAVITINO'` means the same; this also works in the
  bootstrap file's `impl`), the CREATE-time lookup in Gravitino, and allowlist entries that apply
  only when the plugin is enabled. It is compiled against the connector jar (`Provided`,
  `intransitive`) rather than called by reflection, so the API dependency is visible to the
  compiler. It is isolated in `GravitinoBridge` so the jar still loads without Gravitino.
- **Commands log through Spark's `Logging` trait**, which `LeafRunnableCommand` inherits. The logger
  names are `io.subhodeep.sparkext.CreateCatalogCmd`, `…DropCatalogCmd` and `…Bootstrap`.

## Tests

`src/test/scala/io/subhodeep/sparkext/CatalogExtSuite.scala` uses one local session with the
extension chain `IcebergSparkSessionExtensions, DummyParserExtension, CatalogExtension` and a
bootstrap file:

1. CREATE + USE + create table/insert/select
2. IF NOT EXISTS no-op; duplicate fails
3. SHOW CATALOGS contents, columns, order
4. DROP removes conf and cached instance; re-CREATE picks up new options; `spark_catalog` (any case),
   current catalog and missing catalog are refused; plus 4b name validation
5. Allowlist rejection; runtime `SET` cannot widen it
6. `${env:}` substitution (incl. inline), unset variable leaves no partial conf, and secrets are absent
   from captured log4j output, full error stack traces and plan strings; plus 6b unit cases
7. `SELECT 1`, `CREATE TABLE`, `SHOW TABLES`, `SHOW CATALOGS LIKE`, Spark parse errors pass through
8. Coexistence with a dummy parser extension and Iceberg's extension grammar
9. Case, whitespace, comments, `''` escapes, `=`, empty options, syntax-error messages that never echo
   literals
10. Bootstrap file loaded on first statement; plus 10b strict validation without value echo

11. `USING GRAVITINO` parsing, and clear failures when the Gravitino plugin is not enabled

`src/test/scala/io/subhodeep/sparkext/GravitinoSuite.scala` runs against a live Gravitino server
and is skipped (CANCELED) unless `GRAVITINO_IT_URI` and `GRAVITINO_IT_CATALOG_PROPS` are set. It
creates per-run `lakehouse-iceberg` catalogs from a properties template (JDBC backend, S3
warehouse, credential vending), one of them before Spark starts, and deletes them afterwards:

- **G1:** plugin extension rewrite keeps our parser outermost; plugin-registered catalogs show in
  SHOW CATALOGS
- **G2:** attach a catalog created after startup with `USING GRAVITINO`; it starts empty (isolated
  from other catalogs on the same database); create table, insert, select
- **G3:** explicit class must match the provider
- **G4:** unknown catalog fails at CREATE time
- **G5:** DROP and re-attach
- **G6:** a plain Iceberg REST catalog on Gravitino's service sees the same data

Locally, these run against the podman stack in `dev-stack/` (gitignored, local only):

- Gravitino 1.3.1
- SeaweedFS as S3 (MinIO no longer publishes images)
- Postgres for Iceberg JDBC metadata

It also has an `/opt/spark` smoke test and an event-log secret scan; see `dev-stack/README.md`:

```bash
dev-stack/stack.sh up
GRAVITINO_IT_URI=http://127.0.0.1:8090 \
GRAVITINO_IT_CATALOG_PROPS=$PWD/dev-stack/conf/catalog-props-template.json sbt test
dev-stack/spark/run-smoke.sh
```

`dev-stack/` also holds `spark/spark-sql.sh`, a PySpark-based SQL prompt (`-e`, `-f` or
interactive). `/opt/spark` is built without `-Phive-thriftserver`, so it has no `bin/spark-sql`.
The folder also has `spark/examples.sql` and `spark/external-tables.sql` walkthroughs.

Not covered by unit tests: the DROP failure path when reflection fails. Testing it would need
fault injection into `CatalogManagerAccess`.

## Project layout

```
build.sbt                          Spark/Iceberg/Gravitino Provided; tests fork with Java 17 add-opens
project/build.properties           sbt 1.9.9
src/main/scala/io/subhodeep/sparkext/
  CatalogExtension.scala           spark.sql.extensions entry point (injectParser only)
  CatalogExtParser.scala           ParserInterface wrapper + hand-written tokenizer/grammar
  Commands.scala                   CreateCatalogCmd, DropCatalogCmd, ShowCatalogsCmd
  CatalogManagerAccess.scala       the only reflection: evicting cached catalog plugins
  Allowlist.scala                  static-conf allowlist (+ Gravitino classes when the plugin is on)
  Redact.scala                     key-based redaction and ${env:VAR} substitution
  Bootstrap.scala                  phase 2: catalogs from a JSON file on first statement
  GravitinoSupport.scala           USING GRAVITINO; GravitinoBridge is the only Gravitino-linked code
  CatalogExtException.scala        [CATALOGEXT] AnalysisException subclass
src/test/scala/io/subhodeep/sparkext/
  CatalogExtSuite.scala            unit tests 1–11 (local session, Iceberg Hadoop catalog)
  GravitinoSuite.scala             G1–G6 against a live Gravitino (skipped without one)
  DummyParserExtension.scala       second parser extension for the coexistence test
dev-stack/                         local only, gitignored: podman stack, /opt/spark scripts, secrets
```
