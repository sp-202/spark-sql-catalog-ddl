ThisBuild / organization := "io.subhodeep"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.13.8"

val sparkVersion   = "3.5.8"
val icebergVersion = "1.12.0" // latest 1.x with an iceberg-spark-runtime-3.5_2.13 artifact
val gravitinoVersion = "1.3.1" // works with any 1.3.x, see README

lazy val root = (project in file("."))
  .settings(
    name := "spark-catalog-ext",
    javacOptions ++= Seq("--release", "17"),
    scalacOptions ++= Seq("-release", "17", "-target:17", "-deprecation", "-feature", "-unchecked"),
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-sql"                      % sparkVersion   % Provided,
      "org.apache.spark"  %% "spark-catalyst"                 % sparkVersion   % Provided,
      // Shaded fat jar; its declared dependencies are already inside it.
      ("org.apache.gravitino" % "gravitino-spark-connector-runtime-3.5_2.13" % gravitinoVersion % Provided)
        .intransitive(),
      "org.scalatest"     %% "scalatest"                      % "3.2.19"       % Test,
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.13" % icebergVersion % Test,
      // GravitinoSuite only: S3FileIO, and the JDBC driver Gravitino's connector Class.forName()s.
      "org.apache.iceberg" % "iceberg-aws-bundle"             % icebergVersion % Test,
      "org.postgresql"     % "postgresql"                     % "42.7.11"      % Test
    ),
    // Thin jar: nothing but our classes.
    Compile / packageBin / publishArtifact := true,
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "-Xmx2g"
    ),
    // Used by the ${env:VAR} substitution tests.
    Test / envVars := Map("CATALOGEXT_TEST_SECRET" -> "s3cr3t-Value-42", "CATALOGEXT_TEST_PLAIN" -> "hadoop")
  )
