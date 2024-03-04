import sbt._
object Dependencies {
  lazy val logbackVersion = "2.22.1"
  lazy val pureConfigVersion = "0.17.6"
  private val daAwsClientsVersion = "0.1.37"
  private val log4CatsVersion = "2.6.0"

  lazy val awsCrt = "software.amazon.awssdk.crt" % "aws-crt" % "0.29.11"
  lazy val fs2Core = "co.fs2" %% "fs2-core" % "3.9.4"
  lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j-impl" % logbackVersion
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % logbackVersion
  lazy val log4jTemplateJson = "org.apache.logging.log4j" % "log4j-layout-template-json" % logbackVersion
  lazy val log4CatsCore = "org.typelevel" %% "log4cats-core" % log4CatsVersion
  lazy val log4CatsSlf4j = "org.typelevel" %% "log4cats-slf4j" % log4CatsVersion
  lazy val pureConfigCats = "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig" % pureConfigVersion
  lazy val lambdaCore = "com.amazonaws" % "aws-lambda-java-core" % "1.2.3"
  lazy val scalaXml = "org.scala-lang.modules" %% "scala-xml" % "2.2.0"
  lazy val upickle = "com.lihaoyi" %% "upickle" % "3.1.5"
  lazy val dynamoClient = "uk.gov.nationalarchives" %% "da-dynamodb-client" % daAwsClientsVersion
  lazy val dynamoFormatters = "uk.gov.nationalarchives" %% "dynamo-formatters" % "0.0.9"
  lazy val s3Client = "uk.gov.nationalarchives" %% "da-s3-client" % daAwsClientsVersion
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.18"
  lazy val wiremock = "com.github.tomakehurst" % "wiremock" % "3.0.1"
}
