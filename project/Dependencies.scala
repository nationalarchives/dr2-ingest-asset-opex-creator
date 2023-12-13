import sbt._
object Dependencies {
  lazy val logbackVersion = "2.22.0"
  lazy val pureConfigVersion = "0.17.4"
  lazy val fs2Reactive = "co.fs2" %% "fs2-reactive-streams" % "3.9.3"
  lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j-impl" % logbackVersion
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % logbackVersion
  lazy val log4jTemplateJson = "org.apache.logging.log4j" % "log4j-layout-template-json" % logbackVersion
  lazy val pureConfigCats = "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig" % pureConfigVersion
  lazy val lambdaCore = "com.amazonaws" % "aws-lambda-java-core" % "1.2.3"
  lazy val scalaXml = "org.scala-lang.modules" %% "scala-xml" % "2.2.0"
  lazy val upickle = "com.lihaoyi" %% "upickle" % "3.1.3"
  lazy val dynamoClient = "uk.gov.nationalarchives" %% "da-dynamodb-client" % "0.1.32"
  lazy val dynamoFormatters = "uk.gov.nationalarchives" %% "dynamo-formatters" % "0.0.5"
  lazy val s3Client = "uk.gov.nationalarchives" %% "da-s3-client" % "0.1.32"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.17"
  lazy val wiremock = "com.github.tomakehurst" % "wiremock" % "3.0.1"
}
