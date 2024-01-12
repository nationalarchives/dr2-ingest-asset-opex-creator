package uk.gov.nationalarchives

import cats.effect.unsafe.implicits.global
import cats.effect._
import cats.effect.kernel.Resource
import cats.implicits._
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import org.scanamo.syntax._
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import uk.gov.nationalarchives.DADynamoDBClient._
import uk.gov.nationalarchives.DynamoFormatters._
import uk.gov.nationalarchives.Lambda._
import upickle.default._
import fs2.Stream
import org.reactivestreams.{FlowAdapters, Publisher}
import software.amazon.awssdk.transfer.s3.model.CompletedUpload

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.UUID

class Lambda extends RequestStreamHandler {
  val dynamoClient: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  val s3Client: DAS3Client[IO] = DAS3Client[IO]()

  def getXmlCreator: XMLCreator = {
    val ingestDateTime: OffsetDateTime = OffsetDateTime.now()
    XMLCreator(ingestDateTime)
  }

  override def handleRequest(inputStream: InputStream, outputStream: OutputStream, context: Context): Unit = {
    val inputString = inputStream.readAllBytes().map(_.toChar).mkString
    val input = read[Input](inputString)
    val xmlCreator = getXmlCreator
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      assetItems <- dynamoClient.getItems[AssetDynamoTable, PartitionKey](List(PartitionKey(input.id)), config.dynamoTableName)
      asset <- IO.fromOption(assetItems.headOption)(
        new Exception(s"No asset found for ${input.id} and ${input.batchId}")
      )
      _ <- if (asset.`type` != Asset) IO.raiseError(new Exception(s"Object ${asset.id} is of type ${asset.`type`} and not 'Asset'")) else IO.unit
      children <- childrenOfAsset(asset, config.dynamoTableName, config.dynamoGsiName)
      _ <- IO.fromOption(children.headOption)(new Exception(s"No children found for ${input.id} and ${input.batchId}"))
      _ <- children.map(child => copyFromSourceToDestination(input, config.destinationBucket, asset, child, xmlCreator)).sequence
      xip <- xmlCreator.createXip(asset, children.sortBy(_.sortOrder))
      _ <- uploadXMLToS3(xip, config.destinationBucket, s"${assetPath(input, asset)}/${asset.id}.xip")
      opex <- xmlCreator.createOpex(asset, children, xip.getBytes.length, asset.identifiers)
      _ <- uploadXMLToS3(opex, config.destinationBucket, s"${parentPath(input, asset)}/${asset.id}.pax.opex")
    } yield ()
  }.unsafeRunSync()

  private def uploadXMLToS3(xmlString: String, destinationBucket: String, key: String): IO[CompletedUpload] =
    Stream.emits[IO, Byte](xmlString.getBytes).chunks.map(_.toByteBuffer).toPublisherResource.use { publisher =>
      s3Client.upload(destinationBucket, key, xmlString.getBytes.length, publisher)
    }

  private def copyFromSourceToDestination(input: Input, destinationBucket: String, asset: AssetDynamoTable, child: FileDynamoTable, xmlCreator: XMLCreator) = {
    s3Client.copy(
      input.sourceBucket,
      s"${input.batchId}/data/${child.id}",
      destinationBucket,
      destinationPath(input, asset, child, xmlCreator)
    )
  }

  private def parentPath(input: Input, asset: AssetDynamoTable) = s"opex/${input.executionName}${asset.parentPath.map(path => s"/$path").getOrElse("")}"

  private def assetPath(input: Input, asset: AssetDynamoTable) = s"${parentPath(input, asset)}/${asset.id}.pax"

  private def destinationPath(input: Input, asset: AssetDynamoTable, child: FileDynamoTable, xmlCreator: XMLCreator) =
    s"${assetPath(input, asset)}/${xmlCreator.bitstreamPath(child)}/${xmlCreator.childFileName(child)}"

  private def childrenOfAsset(asset: AssetDynamoTable, tableName: String, gsiName: String): IO[List[FileDynamoTable]] = {
    val childrenParentPath = s"${asset.parentPath.map(path => s"$path/").getOrElse("")}${asset.id}"
    dynamoClient
      .queryItems[FileDynamoTable](tableName, gsiName, "batchId" === asset.batchId and "parentPath" === childrenParentPath)
  }
}

object Lambda {
  implicit class StreamToPublisher(stream: Stream[IO, ByteBuffer]) {
    def toPublisherResource: Resource[IO, Publisher[ByteBuffer]] =
      fs2.interop.flow.toPublisher(stream).map(pub => FlowAdapters.toPublisher[ByteBuffer](pub))
  }

  implicit val treInputReader: Reader[Input] = macroR[Input]

  case class Input(id: UUID, batchId: String, executionName: String, sourceBucket: String)

  private case class Config(dynamoTableName: String, dynamoGsiName: String, destinationBucket: String)
}
