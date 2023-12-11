package uk.gov.nationalarchives

import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import uk.gov.nationalarchives.DynamoFormatters._

import java.time.OffsetDateTime
import java.util.UUID
import scala.xml.{Elem, PrettyPrinter}

class XMLCreatorTest extends AnyFlatSpec {

  private val prettyPrinter = new PrettyPrinter(200, 2)
  private val opexNamespace = "http://www.openpreservationexchange.org/opex/v1.2"
  private val ingestDateTime = OffsetDateTime.parse("2023-12-04T10:55:44.848622Z")

  val expectedOpexXml: Elem = <opex:OPEXMetadata xmlns:opex={opexNamespace}>
    <opex:DescriptiveMetadata>
      <Source xmlns="http://dr2.nationalarchives.gov.uk/source">
        <DigitalAssetSource>digitalAssetSource</DigitalAssetSource>
        <DigitalAssetSubtype>digitalAssetSubtype</DigitalAssetSubtype>
        <IngestDateTime>{ingestDateTime}</IngestDateTime>
        <OriginalFiles>
          <File>dec2b921-20e3-41e8-a299-f3cbc13131a2</File>
        </OriginalFiles>
        <OriginalMetadataFiles>
          <File>3f42e3f2-fffe-4fe9-87f7-262e95b86d75</File>
        </OriginalMetadataFiles>
        <TransferDateTime>2023-06-01T00:00Z</TransferDateTime>
        <TransferringBody>transferringBody</TransferringBody>
        <UpstreamSystem>upstreamSystem</UpstreamSystem>
        <UpstreamSystemRef>testSystemRef2</UpstreamSystemRef>
      </Source>
    </opex:DescriptiveMetadata>
    <opex:Transfer>
      <opex:Manifest>
        <opex:Folders>
          <opex:Folder>Representation_Preservation/a814ee41-89f4-4975-8f92-303553fe9a02/Generation_1</opex:Folder>
          <opex:Folder>Representation_Preservation/a814ee41-89f4-4975-8f92-303553fe9a02</opex:Folder>
          <opex:Folder>Representation_Preservation/9ecbba86-437f-42c6-aeba-e28b678bbf4c/Generation_1</opex:Folder>
          <opex:Folder>Representation_Preservation/9ecbba86-437f-42c6-aeba-e28b678bbf4c</opex:Folder>
          <opex:Folder>Representation_Preservation</opex:Folder>
        </opex:Folders>
        <opex:Files>
          <opex:File type="metadata" size="4">90730c77-8faa-4dbf-b20d-bba1046dac87.xip</opex:File>
          <opex:File type="content" size="1">Representation_Preservation/a814ee41-89f4-4975-8f92-303553fe9a02/Generation_1/a814ee41-89f4-4975-8f92-303553fe9a02.ext0</opex:File>
          <opex:File type="content" size="1">Representation_Preservation/9ecbba86-437f-42c6-aeba-e28b678bbf4c/Generation_1/9ecbba86-437f-42c6-aeba-e28b678bbf4c.ext1</opex:File>
        </opex:Files>
      </opex:Manifest>
    </opex:Transfer>
    <opex:Properties>
      <opex:Title>title</opex:Title>
      <opex:Description>description</opex:Description>
      <opex:SecurityDescriptor>open</opex:SecurityDescriptor>
      <opex:Identifiers>
        <opex:Identifier type="Test1">Value1</opex:Identifier>
        <opex:Identifier type="Test2">Value2</opex:Identifier>
        <opex:Identifier type="UpstreamSystemReference">testSystemRef2</opex:Identifier>
      </opex:Identifiers>
    </opex:Properties>
  </opex:OPEXMetadata>

  val expectedXipXml: Elem = <XIP xmlns="http://preservica.com/XIP/v6.4">
    <InformationObject>
      <Ref>90730c77-8faa-4dbf-b20d-bba1046dac87</Ref>
      <SecurityTag>open</SecurityTag>
      <Title>Preservation</Title>
    </InformationObject>
    <Representation>
      <InformationObject>90730c77-8faa-4dbf-b20d-bba1046dac87</InformationObject>
      <Type>Preservation</Type>
      <Name>Preservation</Name>
      <ContentObjects>
        <ContentObject>a814ee41-89f4-4975-8f92-303553fe9a02</ContentObject>
        <ContentObject>9ecbba86-437f-42c6-aeba-e28b678bbf4c</ContentObject>
      </ContentObjects>
    </Representation>
    <ContentObject>
      <Ref>a814ee41-89f4-4975-8f92-303553fe9a02</Ref>
      <Title>name0</Title>
      <Parent>90730c77-8faa-4dbf-b20d-bba1046dac87</Parent>
      <SecurityTag>open</SecurityTag>
    </ContentObject>
    <Generation original="true" active="true">
      <ContentObject>a814ee41-89f4-4975-8f92-303553fe9a02</ContentObject>
      <Bitstreams>
        <Bitstream>Representation_Preservation/a814ee41-89f4-4975-8f92-303553fe9a02/Generation_1/a814ee41-89f4-4975-8f92-303553fe9a02.ext0</Bitstream>
      </Bitstreams>
    </Generation>
    <Bitstream>
      <Filename>a814ee41-89f4-4975-8f92-303553fe9a02.ext0</Filename>
      <FileSize>1</FileSize>
      <PhysicalLocation>Representation_Preservation/a814ee41-89f4-4975-8f92-303553fe9a02/Generation_1</PhysicalLocation>
      <Fixities>
        <Fixity>
          <FixityAlgorithmRef>SHA256</FixityAlgorithmRef>
          <FixityValue>checksum0</FixityValue>
        </Fixity>
      </Fixities>
    </Bitstream>
    <ContentObject>
      <Ref>9ecbba86-437f-42c6-aeba-e28b678bbf4c</Ref>
      <Title>name1</Title>
      <Parent>90730c77-8faa-4dbf-b20d-bba1046dac87</Parent>
      <SecurityTag>open</SecurityTag>
    </ContentObject>
    <Generation original="true" active="true">
      <ContentObject>9ecbba86-437f-42c6-aeba-e28b678bbf4c</ContentObject>
      <Bitstreams>
        <Bitstream>Representation_Preservation/9ecbba86-437f-42c6-aeba-e28b678bbf4c/Generation_1/9ecbba86-437f-42c6-aeba-e28b678bbf4c.ext1</Bitstream>
      </Bitstreams>
    </Generation>
    <Bitstream>
      <Filename>9ecbba86-437f-42c6-aeba-e28b678bbf4c.ext1</Filename>
      <FileSize>1</FileSize>
      <PhysicalLocation>Representation_Preservation/9ecbba86-437f-42c6-aeba-e28b678bbf4c/Generation_1</PhysicalLocation>
      <Fixities>
        <Fixity>
          <FixityAlgorithmRef>SHA256</FixityAlgorithmRef>
            <FixityValue>checksum1</FixityValue>
          </Fixity>
        </Fixities>
      </Bitstream>
  </XIP>

  val asset: AssetDynamoTable = AssetDynamoTable(
    "TEST-ID",
    UUID.fromString("90730c77-8faa-4dbf-b20d-bba1046dac87"),
    Option("parentPath"),
    "name",
    Asset,
    Option("title"),
    Option("description"),
    "transferringBody",
    OffsetDateTime.parse("2023-06-01T00:00Z"),
    "upstreamSystem",
    "digitalAssetSource",
    "digitalAssetSubtype",
    List(UUID.fromString("dec2b921-20e3-41e8-a299-f3cbc13131a2")),
    List(UUID.fromString("3f42e3f2-fffe-4fe9-87f7-262e95b86d75")),
    List(Identifier("Test2", "testIdentifier2"), Identifier("Test", "testIdentifier"), Identifier("UpstreamSystemReference", "testSystemRef"))
  )
  val uuids: List[UUID] = List(UUID.fromString("a814ee41-89f4-4975-8f92-303553fe9a02"), UUID.fromString("9ecbba86-437f-42c6-aeba-e28b678bbf4c"))
  val children: List[FileDynamoTable] = uuids.zipWithIndex.map { case (uuid, suffix) =>
    FileDynamoTable(
      "TEST-ID",
      uuid,
      Option(s"parentPath$suffix"),
      s"name$suffix",
      File,
      Option(s"title$suffix"),
      Option(s"description$suffix"),
      suffix,
      1,
      s"checksum$suffix",
      s"ext$suffix",
      List(Identifier("Test2", "testIdentifier4"), Identifier("Test", "testIdentifier3"), Identifier("UpstreamSystemReference", "testSystemRef2"))
    )
  }

  "createOpex" should "create the correct opex xml with identifiers" in {
    val identifiers = List(Identifier("Test1", "Value1"), Identifier("Test2", "Value2"), Identifier("UpstreamSystemReference", "testSystemRef2"))
    val xml = XMLCreator(ingestDateTime).createOpex(asset, children, 4, identifiers).unsafeRunSync()
    xml should equal(prettyPrinter.format(expectedOpexXml))
  }

  "createOpex" should "throw a 'NoSuchElementException' if the identifiers the opex need are missing" in {
    val ex = intercept[NoSuchElementException] {
      XMLCreator(ingestDateTime).createOpex(asset, children, 4, Nil).unsafeRunSync()
    }
    ex.getMessage should equal("None.get")
  }

  "createXip" should "create the correct xip xml" in {
    val xml = XMLCreator(ingestDateTime).createXip(asset, children).unsafeRunSync()
    xml should equal(prettyPrinter.format(expectedXipXml) + "\n")
  }
}
