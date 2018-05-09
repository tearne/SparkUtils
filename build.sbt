name := "sparkutils"

version := "0.0.1"

scalaVersion := "2.11.12"

organization := "org.tearne"

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

resolvers ++= Seq(
  Resolver.bintrayRepo("tearne", "maven")
)

libraryDependencies ++= Seq(
  "org.tearne" %% "sampler-core" % "0.3.15",
  "commons-io" % "commons-io" % "2.4",
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.slf4j" % "slf4j-api" % "1.7.12"
)
