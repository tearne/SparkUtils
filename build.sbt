val projectName = "sparkutils"
val projectVersion = "0.0.8"
val projectOrg = "org.tearne"
val projectLicenses = ("MIT", url("http://opensource.org/licenses/MIT"))

val buildScalaVersion = "2.12.8"
val crossBuildVersions = Seq(buildScalaVersion, "2.11.12")

val myResolvers = Seq(
  Resolver.bintrayRepo("tearne", "maven")
)

val myDeps = Seq(
  "org.tearne" %% "sampler-core" % "0.3.15",
  "commons-io" % "commons-io" % "2.4",
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.slf4j" % "slf4j-api" % "1.7.12",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.9",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.9",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.9"
)

lazy val root = (project in file("."))
    .settings(
      name := projectName,
      version := projectVersion,
      licenses += projectLicenses,
      scalaVersion := buildScalaVersion,
      crossScalaVersions := crossBuildVersions,

      resolvers ++= myResolvers,
      libraryDependencies ++= myDeps,
//      assemblyMergeStrategy in assembly := {
//        case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//        case x => MergeStrategy.first
//      }
    )