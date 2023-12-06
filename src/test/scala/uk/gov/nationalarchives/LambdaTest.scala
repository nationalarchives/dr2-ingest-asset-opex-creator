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
import java.time.OffsetDateTime
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.xml.{PrettyPrinter, XML}

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

  def stubPutRequest(): (String, String) = {
    val xipPath = s"/opex/$executionName/$assetParentPath/$assetId.pax/$assetId.xip"
    val opexPath = s"/opex/$executionName/$assetParentPath/$assetId.pax.opex"
    List(xipPath, opexPath).foreach { itemPath =>
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
    (xipPath, opexPath)
  }

  def stubJsonCopyRequest(): (String, String) = stubCopyRequest(childIdJson, "json")

  def stubDocxCopyRequest(): (String, String) = stubCopyRequest(childIdDocx, "docx")

  def stubCopyRequest(childId: UUID, suffix: String): (String, String) = {
    val sourceName = s"/$batchId/data/$childId"
    val destinationName = s"/opex/$executionName/$assetParentPath/$assetId.pax/Representation_Preservation/$childId/Generation_1/$childId.$suffix"
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
    (sourceName, destinationName)
  }

  def stubGetRequest(batchGetResponse: String): Unit =
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.RequestItems", containing("test-table")))
        .willReturn(ok().withBody(batchGetResponse))
    )

  def stubPostRequest(postResponse: String): Unit =
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.TableName", equalTo("test-table")))
        .willReturn(ok().withBody(postResponse))
    )

  private val expectedOpex = <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.2">
    <DescriptiveMetadata>
      <Source xmlns="http://dr2.nationalarchives.gov.uk/source">
        <DigitalAssetSource>Test Digital Asset Source</DigitalAssetSource>
        <DigitalAssetSubtype>Test Digital Asset Subtype</DigitalAssetSubtype>
        <IngestDateTime>2023-09-01T00:00Z</IngestDateTime>
        <OriginalFiles>
          <File>b6102810-53e3-43a2-9f69-fafe71d4aa40</File>
        </OriginalFiles>
        <OriginalMetadataFiles>
          <File>c019df6a-fccd-4f81-86d8-085489fc71db</File>
        </OriginalMetadataFiles>
        <TransferDateTime>2023-08-01T00:00Z</TransferDateTime>
        <TransferringBody>Test Transferring Body</TransferringBody>
        <UpstreamSystem>Test Upstream System</UpstreamSystem>
        <UpstreamSystemRef>UpstreamSystemReference</UpstreamSystemRef>
      </Source>
    </DescriptiveMetadata>
    <opex:Transfer>
      <opex:Manifest>
        <opex:Folders>
          <opex:Folder>Representation_Preservation/feedd76d-e368-45c8-96e3-c37671476793/Generation_1</opex:Folder>
          <opex:Folder>Representation_Preservation/feedd76d-e368-45c8-96e3-c37671476793</opex:Folder>
          <opex:Folder>Representation_Preservation/a25d33f3-7726-4fb3-8e6f-f66358451c4e</opex:Folder>
          <opex:Folder>Representation_Preservation/a25d33f3-7726-4fb3-8e6f-f66358451c4e/Generation_1</opex:Folder>
          <opex:Folder>Representation_Preservation</opex:Folder>
        </opex:Folders>
        <opex:Files>
          <opex:File type="metadata" size="2463">68b1c80b-36b8-4f0f-94d6-92589002d87e.xip</opex:File>
          <opex:File type="content" size="1">Representation_Preservation/a25d33f3-7726-4fb3-8e6f-f66358451c4e/Generation_1/a25d33f3-7726-4fb3-8e6f-f66358451c4e.docx</opex:File>
          <opex:File type="content" size="2">Representation_Preservation/feedd76d-e368-45c8-96e3-c37671476793/Generation_1/feedd76d-e368-45c8-96e3-c37671476793.json</opex:File>
        </opex:Files>
      </opex:Manifest>
    </opex:Transfer>
    <opex:Properties>
      <opex:Title>Test Name</opex:Title>
      <opex:Description/>
      <opex:SecurityDescriptor>open</opex:SecurityDescriptor>
      <Identifiers>
        <Identifier type="UpstreamSystemReference">UpstreamSystemReference</Identifier>
        <Identifier type="Code">Code</Identifier>
      </Identifiers>
    </opex:Properties>
  </opex:OPEXMetadata>

  val assetId: UUID = UUID.fromString("68b1c80b-36b8-4f0f-94d6-92589002d87e")
  val assetParentPath: String = "a/parent/path"
  val childIdJson: UUID = UUID.fromString("feedd76d-e368-45c8-96e3-c37671476793")
  val childIdDocx: UUID = UUID.fromString("a25d33f3-7726-4fb3-8e6f-f66358451c4e")
  val batchId: String = "TEST-ID"
  val executionName = "test-execution"
  val inputJson: String = s"""{"batchId": "$batchId", "id": "$assetId", "executionName": "$executionName", "sourceBucket": "test-source-bucket"}"""

  def standardInput: ByteArrayInputStream = new ByteArrayInputStream(inputJson.getBytes)

  def outputStream: ByteArrayOutputStream = new ByteArrayOutputStream()

  val emptyDynamoGetResponse: String = """{"Responses": {"test-table": []}}"""
  val emptyDynamoPostResponse: String = """{"Count": 0, "Items": []}"""
  val dynamoPostResponse: String =
    s"""{
      |  "Count": 2,
      |  "Items": [
      |    {
      |      "checksum_sha256": {
      |        "S": "checksumdocx"
      |      },
      |      "fileExtension": {
      |        "S": "docx"
      |      },
      |      "fileSize": {
      |        "N": "1"
      |      },
      |      "sortOrder": {
      |        "N": "1"
      |      },
      |      "id": {
      |        "S": "$childIdDocx"
      |      },
      |      "parentPath": {
      |        "S": "parent/path"
      |      },
      |      "name": {
      |        "S": "$batchId.docx"
      |      },
      |      "type": {
      |        "S": "File"
      |      },
      |      "batchId": {
      |        "S": "$batchId"
      |      },
      |      "transferringBody": {
      |        "S": "Test Transferring Body"
      |      },
      |      "transferCompleteDatetime": {
      |        "S": "2023-09-01T00:00Z"
      |      },
      |      "upstreamSystem": {
      |        "S": "Test Upstream System"
      |      },
      |      "digitalAssetSource": {
      |        "S": "Test Digital Asset Source"
      |      },
      |      "digitalAssetSubtype": {
      |        "S": "Test Digital Asset Subtype"
      |      },
      |      "originalFiles": {
      |        "L": [ { "S" : "b6102810-53e3-43a2-9f69-fafe71d4aa40" } ]
      |      },
      |      "originalMetadataFiles": {
      |        "L": [ { "S" : "c019df6a-fccd-4f81-86d8-085489fc71db" } ]
      |      },
      |      "id_Code": {
      |          "S": "Code"
      |      },
      |      "id_UpstreamSystemReference": {
      |        "S": "UpstreamSystemReference"
      |      }
      |    },
      |    {
      |      "checksum_sha256": {
      |        "S": "checksum"
      |      },
      |      "fileExtension": {
      |        "S": "json"
      |      },
      |      "fileSize": {
      |        "N": "2"
      |      },
      |      "sortOrder": {
      |        "N": "2"
      |      },
      |      "id": {
      |        "S": "$childIdJson"
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
      |      },
      |      "transferringBody": {
      |        "S": "Test Transferring Body"
      |      },
      |      "transferCompleteDatetime": {
      |        "S": "2023-09-01T00:00Z"
      |      },
      |      "upstreamSystem": {
      |        "S": "Test Upstream System"
      |      },
      |      "digitalAssetSource": {
      |        "S": "Test Digital Asset Source"
      |      },
      |      "digitalAssetSubtype": {
      |        "S": "Test Digital Asset Subtype"
      |      },
      |      "originalFiles": {
      |        "L": [ { "S" : "b6102810-53e3-43a2-9f69-fafe71d4aa40" } ]
      |      },
      |      "originalMetadataFiles": {
      |        "L": [ { "S" : "c019df6a-fccd-4f81-86d8-085489fc71db" } ]
      |      },
      |      "id_Code": {
      |          "S": "Code"
      |      },
      |      "id_UpstreamSystemReference": {
      |        "S": "UpstreamSystemReference"
      |      }
      |    }
      |  ]
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
       |        },
       |        "transferringBody": {
       |          "S": "Test Transferring Body"
       |        },
       |        "transferCompleteDatetime": {
       |          "S": "2023-08-01T00:00Z"
       |        },
       |        "upstreamSystem": {
       |          "S": "Test Upstream System"
       |        },
       |        "digitalAssetSource": {
       |          "S": "Test Digital Asset Source"
       |        },
       |        "digitalAssetSubtype": {
       |          "S": "Test Digital Asset Subtype"
       |        },
       |        "originalFiles": {
       |          "L": [ { "S" : "b6102810-53e3-43a2-9f69-fafe71d4aa40" } ]
       |        },
       |        "originalMetadataFiles": {
       |          "L": [ { "S" : "c019df6a-fccd-4f81-86d8-085489fc71db" } ]
       |        },
       |        "id_Code": {
       |          "S": "Code"
       |        },
       |        "id_UpstreamSystemReference": {
       |          "S": "UpstreamSystemReference"
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
    override val ingestDateTime: OffsetDateTime = OffsetDateTime.parse("2023-09-01T00:00Z")
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
    stubPostRequest(emptyDynamoPostResponse)
    val ex = intercept[Exception] {
      TestLambda().handleRequest(standardInput, outputStream, null)
    }
    ex.getMessage should equal(s"No children found for $assetId and $batchId")
  }

  "handleRequest" should "return an error if the dynamo entry does not have a type of 'Asset'" in {
    stubGetRequest(dynamoGetResponse.replace(""""S": "Asset"""", """"S": "ArchiveFolder""""))
    stubPostRequest(emptyDynamoPostResponse)
    val ex = intercept[Exception] {
      TestLambda().handleRequest(standardInput, outputStream, null)
    }
    ex.getMessage should equal(s"Object $assetId is of type ArchiveFolder and not 'Asset'")
  }

  "handleRequest" should "pass the correct id to dynamo getItem" in {
    stubGetRequest(emptyDynamoGetResponse)
    intercept[Exception] {
      TestLambda().handleRequest(standardInput, outputStream, null)
    }
    val serveEvents = dynamoServer.getAllServeEvents.asScala
    serveEvents.size should equal(1)
    serveEvents.head.getRequest.getBodyAsString should equal(s"""{"RequestItems":{"test-table":{"Keys":[{"id":{"S":"$assetId"}}]}}}""")
  }

  "handleRequest" should "pass the correct parameters to dynamo for the query request" in {
    stubGetRequest(dynamoGetResponse)
    stubPostRequest(emptyDynamoPostResponse)
    intercept[Exception] {
      TestLambda().handleRequest(standardInput, outputStream, null)
    }
    val serveEvents = dynamoServer.getAllServeEvents.asScala
    val queryEvent = serveEvents.head
    val requestBody = queryEvent.getRequest.getBodyAsString
    val expectedRequestBody =
      """{"TableName":"test-table","IndexName":"test-gsi","KeyConditionExpression":"#A = :batchId AND #B = :parentPath",""" +
        s""""ExpressionAttributeNames":{"#A":"batchId","#B":"parentPath"},"ExpressionAttributeValues":{":batchId":{"S":"TEST-ID"},":parentPath":{"S":"$assetParentPath/$assetId"}}}"""
    requestBody should equal(expectedRequestBody)
  }

  "handleRequest" should "copy the correct child assets from source to destination" in {
    stubGetRequest(dynamoGetResponse)
    stubPostRequest(dynamoPostResponse)
    val (sourceJson, destinationJson) = stubJsonCopyRequest()
    val (sourceDocx, destinationDocx) = stubDocxCopyRequest()
    stubPutRequest()

    TestLambda().handleRequest(standardInput, outputStream, null)

    def checkCopyRequest(source: String, destination: String) = {
      val s3CopyRequest = s3Server.getAllServeEvents.asScala.filter(_.getRequest.getUrl == destination).head.getRequest
      s3CopyRequest.getUrl should equal(destination)
      s3CopyRequest.getHost should equal("test-destination-bucket.localhost")
      s3CopyRequest.getHeader("x-amz-copy-source") should equal(s"test-source-bucket$source")
    }
    checkCopyRequest(sourceJson, destinationJson)
    checkCopyRequest(sourceDocx, destinationDocx)
  }

  "handleRequest" should "upload the xip and opex files" in {
    stubGetRequest(dynamoGetResponse)
    stubPostRequest(dynamoPostResponse)
    val (xipPath, opexPath) = stubPutRequest()
    stubJsonCopyRequest()
    stubDocxCopyRequest()

    TestLambda().handleRequest(standardInput, outputStream, null)

    val s3CopyRequests = s3Server.getAllServeEvents.asScala
    s3CopyRequests.count(_.getRequest.getUrl == xipPath) should equal(1)
    s3CopyRequests.count(_.getRequest.getUrl == opexPath) should equal(1)
  }

  "handleRequest" should "write the xip content objects in the correct order" in {
    stubGetRequest(dynamoGetResponse)
    stubPostRequest(dynamoPostResponse)
    val (xipPath, _) = stubPutRequest()
    stubJsonCopyRequest()
    stubDocxCopyRequest()

    TestLambda().handleRequest(standardInput, outputStream, null)

    val s3CopyRequests = s3Server.getAllServeEvents.asScala
    val xipString = s3CopyRequests.filter(_.getRequest.getUrl == xipPath).head.getRequest.getBodyAsString.split("\n").tail.dropRight(4).mkString("\n")
    val contentObjects = XML.loadString(xipString) \ "Representation" \ "ContentObjects" \ "ContentObject"
    contentObjects.head.text should equal(childIdDocx.toString)
    contentObjects.last.text should equal(childIdJson.toString)
  }

  "handleRequest" should "upload the correct opex file to s3" in {
    stubGetRequest(dynamoGetResponse)
    stubPostRequest(dynamoPostResponse)
    val (_, opexPath) = stubPutRequest()
    stubJsonCopyRequest()
    stubDocxCopyRequest()
    val prettyPrinter = new PrettyPrinter(180, 2)

    TestLambda().handleRequest(standardInput, outputStream, null)

    val s3UploadRequests = s3Server.getAllServeEvents.asScala
    val opexString = s3UploadRequests.filter(_.getRequest.getUrl == opexPath).head.getRequest.getBodyAsString.split("\n").tail.dropRight(3).mkString("\n")
    val opexXml = XML.loadString(opexString)
    prettyPrinter.format(opexXml) should equal(prettyPrinter.format(expectedOpex))
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
    stubPostRequest(dynamoPostResponse)
    val ex = intercept[Exception] {
      TestLambda().handleRequest(standardInput, outputStream, null)
    }
    ex.getMessage should equal("Failed to retrieve metadata from the source object")
  }
}
