import Dependencies._
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file(".")).settings(
  name := "dr2-ingest-asset-opex-creator",
  resolvers += "s01-oss-sonatype-org-snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots",
  libraryDependencies ++= Seq(
    fs2Core,
    log4jSlf4j,
    log4jCore,
    log4jTemplateJson,
    lambdaCore,
    dynamoClient,
    dynamoFormatters,
    pureConfig,
    pureConfigCats,
    scalaXml,
    s3Client,
    upickle,
    wiremock % Test,
    scalaTest % Test
  ),
  scalacOptions += "-deprecation"
)
(assembly / assemblyJarName) := "dr2-ingest-asset-opex-creator.jar"

scalacOptions ++= Seq("-Wunused:imports", "-Werror")

(assembly / assemblyMergeStrategy) := {
  case PathList(ps @ _*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
  case _                                                   => MergeStrategy.first
}
