name := "sparkutils"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.12"

organization := "tearne"

resolvers ++= Seq(
  Resolver.bintrayRepo("tearne", "maven")
)

libraryDependencies ++= Seq(
  "org.tearne" %% "sampler-core" % "0.3.15",
  "commons-io" % "commons-io" % "2.4",
  "org.threeten" % "threeten-extra" % "1.3.2",
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.hadoop" % "hadoop-aws" % "2.8.3",
  "org.apache.hadoop" % "hadoop-client" % "2.8.3",
  "org.slf4j" % "slf4j-api" % "1.7.12"
)
