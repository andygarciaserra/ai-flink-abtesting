ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "1.0"
ThisBuild / organization := "es.dmr.uimp"

lazy val flinkVersion = "1.16.3"

lazy val root = (project in file("."))
  .settings(
    name := "flink-abtesting",

    libraryDependencies ++= Seq(
      // Flink runtime/API. Marked as provided because a real Flink cluster already provides them.
      "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
      "org.apache.flink" % "flink-clients" % flinkVersion % "provided",

      // Kafka connector used to read input topics and write the prediction topic.
      "org.apache.flink" % "flink-connector-kafka" % flinkVersion,

      // JSON parsing/serialization.
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.5",

      // XML parsing for the PMML file.
      "org.scala-lang.modules" %% "scala-xml" % "2.1.0"
    ),

    Compile / mainClass := Some("es.dmr.uimp.flink.StudentPredictionJob"),
    assembly / mainClass := Some("es.dmr.uimp.flink.StudentPredictionJob"),
    assembly / assemblyJarName := "flink-abtesting-assembly-1.0.jar",

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf"             => MergeStrategy.concat
      case x                             => MergeStrategy.first
    }
  )
