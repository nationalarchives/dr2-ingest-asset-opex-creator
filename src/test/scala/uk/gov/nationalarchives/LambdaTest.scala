package uk.gov.nationalarchives

import cats.effect.IO
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClient

import java.io.ByteArrayInputStream
import java.net.URI
import java.util.UUID
import scala.jdk.CollectionConverters._

class LambdaTest extends AnyFlatSpec with BeforeAndAfterEach {
  val dynamoServer = new WireMockServer(9005)
  val s3Server = new WireMockServer(9006)

  override def beforeEach(): Unit = {
    dynamoServer.start()
    s3Server.start()
  }

  override def afterEach(): Unit = {
    dynamoServer.resetAll()
    s3Server.resetAll()
    dynamoServer.stop()
    s3Server.stop()
  }

  def stubPutRequest(itemPaths: String*): Unit = {
    itemPaths.foreach { itemPath =>
      s3Server.stubFor(
        put(urlEqualTo(itemPath))
          .withHost(equalTo("test-destination-bucket.localhost"))
          .willReturn(ok())
      )
      s3Server.stubFor(
        head(urlEqualTo(s"/$itemPath"))
          .willReturn(ok())
      )
    }
  }

  def stubCopyRequest(sourceName: String, destinationName: String): Unit = {
    val response =
      <CopyObjectResult>
        <LastModified>2023-08-29T17:50:30.000Z</LastModified>
        <ETag>"9b2cf535f27731c974343645a3985328"</ETag>
      </CopyObjectResult>
    s3Server.stubFor(
      head(urlEqualTo(destinationName))
        .willReturn(ok().withHeader("Content-Length", "1"))
    )
    s3Server.stubFor(
      head(urlEqualTo(sourceName))
        .willReturn(ok().withHeader("Content-Length", "1"))
    )
    s3Server.stubFor(
      put(urlEqualTo(destinationName))
        .withHost(equalTo("test-destination-bucket.localhost"))
        .withHeader("x-amz-copy-source", equalTo(s"test-source-bucket$sourceName"))
        .willReturn(okXml(response.toString()))
    )
  }

  def stubGetRequest(batchGetResponse: String): Unit =
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.RequestItems", containing("test-table")))
        .willReturn(ok().withBody(batchGetResponse))
    )

  def stubScanRequest(scanResponse: String): Unit =
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.TableName", equalTo("test-table")))
        .willReturn(ok().withBody(scanResponse))
    )

  val assetId: UUID = UUID.fromString("68b1c80b-36b8-4f0f-94d6-92589002d87e")
  val assetParentPath: String = "a/parent/path"
  val childId: UUID = UUID.fromString("feedd76d-e368-45c8-96e3-c37671476793")
  val batchId: String = "TEST-ID"
  val executionName = "test-execution"
  val inputJson: String = s"""{"batchId": "$batchId", "id": "$assetId", "executionName": "$executionName"}"""

  def standardInput: ByteArrayInputStream = new ByteArrayInputStream(inputJson.getBytes)

  def outputStream: ByteArrayOutputStream = new ByteArrayOutputStream()

  val emptyDynamoGetResponse: String = """{"Responses": {"test-table": []}}"""
  val emptyDynamoScanResponse: String = """{"Count": 0, "Items": []}"""
  val dynamoScanResponse: String =
    s"""{
      |  "Count": 2,
      |  "Items": [
      |    {
      |      "checksumSha256": {
      |        "S": "checksum"
      |      },
      |      "fileExtension": {
      |        "S": "json"
      |      },
      |      "fileSize": {
      |        "N": "1"
      |      },
      |      "id": {
      |        "S": "$childId"
      |      },
      |      "parentPath": {
      |        "S": "parent/path"
      |      },
      |      "name": {
      |        "S": "$batchId.json"
      |      },
      |      "type": {
      |        "S": "File"
      |      },
      |      "batchId": {
      |        "S": "$batchId"
      |      }
      |    }
      |  ],
      |  "ScannedCount": 6
      |}
      |""".stripMargin
  val dynamoGetResponse: String =
    s"""{
       |  "Responses": {
       |    "test-table": [
       |      {
       |        "id": {
       |          "S": "$assetId"
       |        },
       |        "name": {
       |          "S": "Test Name"
       |        },
       |        "parentPath": {
       |          "S": "$assetParentPath"
       |        },
       |        "type": {
       |          "S": "Asset"
       |        },
       |        "batchId": {
       |          "S": "$batchId"
       |        }
       |      }
       |    ]
       |  }
       |}
       |""".stripMargin

  case class TestLambda() extends Lambda {
    val creds: StaticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
    private val asyncDynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient
      .builder()
      .endpointOverride(URI.create("http://localhost:9005"))
      .region(Region.EU_WEST_2)
      .credentialsProvider(creds)
      .build()

    private val asyncS3Client: S3AsyncClient = S3AsyncClient
      .crtBuilder()
      .endpointOverride(URI.create("http://localhost:9006"))
      .region(Region.EU_WEST_2)
      .credentialsProvider(creds)
      .targetThroughputInGbps(20.0)
      .minimumPartSizeInBytes(10 * 1024 * 1024)
      .build()
    override val dynamoClient: DADynamoDBClient[IO] = new DADynamoDBClient[IO](asyncDynamoClient)
    override val s3Client: DAS3Client[IO] = DAS3Client[IO](asyncS3Client)
  }

  "handleRequest" should "return an error if the asset is not found in dynamo" in {
    stubGetRequest(emptyDynamoGetResponse)
    val ex = intercept[Exception] {
      TestLambda().handleRequest(standardInput, outputStream, null)
    }
    ex.getMessage should equal(s"No asset found for $assetId and $batchId")
  }

  "handleRequest" should "return an error if no children are found for the asset" in {
    stubGetRequest(dynamoGetResponse)
    stubScanRequest(emptyDynamoScanResponse)
    val ex = intercept[Exception] {
      TestLambda().handleRequest(standardInput, outputStream, null)
    }
    ex.getMessage should equal(s"No children found for $assetId and $batchId")
  }

  "handleRequest" should "pass the correct id to dynamo get item" in {
    stubGetRequest(emptyDynamoGetResponse)
    intercept[Exception] {
      TestLambda().handleRequest(standardInput, outputStream, null)
    }
    val serveEvents = dynamoServer.getAllServeEvents.asScala
    serveEvents.size should equal(1)
    serveEvents.head.getRequest.getBodyAsString should equal(s"""{"RequestItems":{"test-table":{"Keys":[{"id":{"S":"$assetId"}}]}}}""")
  }

  "handleRequest" should "pass the correct parameters to dynamo for the scan request" in {
    stubGetRequest(dynamoGetResponse)
    stubScanRequest(emptyDynamoScanResponse)
    intercept[Exception] {
      TestLambda().handleRequest(standardInput, outputStream, null)
    }
    val serveEvents = dynamoServer.getAllServeEvents.asScala
    val scanEvent = serveEvents.head
    val requestBody = scanEvent.getRequest.getBodyAsString
    val expectedRequestBody =
      """{"TableName":"test-table","FilterExpression":"#A = :batchId AND #B = :parentPath","ExpressionAttributeNames":{"#A":"batchId","#B":"parentPath"},""" +
        s""""ExpressionAttributeValues":{":batchId":{"S":"$batchId"},":parentPath":{"S":"$assetParentPath/$assetId"}}}"""
    expectedRequestBody should equal(requestBody)
  }

  "handleRequest" should "copy the correct child assets from source to destination" in {
    stubGetRequest(dynamoGetResponse)
    stubScanRequest(dynamoScanResponse)
    val source = s"/$batchId/data/$childId"
    val destination = s"/opex/$executionName/$assetParentPath/$assetId.pax/Representation_Preservation/${childId}/Generation_1/$childId.json"
    stubCopyRequest(source, destination)
    val xipPath = s"/opex/$executionName/$assetParentPath/$assetId.pax/$assetId.xip"
    val opexPath = s"/opex/$executionName/$assetParentPath/$assetId.pax.opex"
    stubPutRequest(xipPath, opexPath)

    TestLambda().handleRequest(standardInput, outputStream, null)

    val s3CopyRequest = s3Server.getAllServeEvents.asScala.filter(_.getRequest.getUrl == destination).head.getRequest
    s3CopyRequest.getUrl should equal(destination)
    s3CopyRequest.getHost should equal("test-destination-bucket.localhost")
    s3CopyRequest.getHeader("x-amz-copy-source") should equal(s"test-source-bucket$source")
  }

  "handleRequest" should "upload the xip and opex files" in {
    stubGetRequest(dynamoGetResponse)
    stubScanRequest(dynamoScanResponse)
    val source = s"/$batchId/data/$childId"
    val destination = s"/opex/$executionName/$assetParentPath/$assetId.pax/Representation_Preservation/$childId/Generation_1/$childId.json"
    val xipPath = s"/opex/$executionName/$assetParentPath/$assetId.pax/$assetId.xip"
    val opexPath = s"/opex/$executionName/$assetParentPath/$assetId.pax.opex"
    stubPutRequest(xipPath, opexPath)
    stubCopyRequest(source, destination)

    TestLambda().handleRequest(standardInput, outputStream, null)

    val s3CopyRequests = s3Server.getAllServeEvents.asScala
    s3CopyRequests.count(_.getRequest.getUrl == xipPath) should equal(1)
    s3CopyRequests.count(_.getRequest.getUrl == opexPath) should equal(1)
  }

  "handleRequest" should "return an error if the Dynamo API is unavailable" in {
    dynamoServer.stop()
    val ex = intercept[Exception] {
      TestLambda().handleRequest(standardInput, outputStream, null)
    }
    ex.getMessage should equal("Unable to execute HTTP request: Connection refused: localhost/127.0.0.1:9005")
  }

  "handleRequest" should "return an error if the S3 API is unavailable" in {
    s3Server.stop()
    stubGetRequest(dynamoGetResponse)
    stubScanRequest(dynamoScanResponse)
    val ex = intercept[Exception] {
      TestLambda().handleRequest(standardInput, outputStream, null)
    }
    ex.getMessage should equal("Failed to retrieve metadata from the source object")
  }
}
