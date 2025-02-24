ThisBuild / scalaVersion     := "2.12.18"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "Example"

lazy val root = (project in file("."))
  .settings(
    name := "ProgettoEsame",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.4",
      "org.apache.spark" %% "spark-sql"  % "3.5.4",
    )
  )

// Per velocizzare la compilazione
ThisBuild / fork := true

// Plugin SBT Assembly
enablePlugins(AssemblyPlugin)

assembly / mainClass := Some("com.example.Main") // Classe principale

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                             => MergeStrategy.first
}
