package uk.gov.nationalarchives

import cats.effect.IO
import uk.gov.nationalarchives.DynamoFormatters._

import java.time.OffsetDateTime
import scala.xml._

class XMLCreator(ingestDateTime: OffsetDateTime) {
  private val prettyPrinter = new PrettyPrinter(200, 2)
  private val opexNamespace = "http://www.openpreservationexchange.org/opex/v1.2"
  private[nationalarchives] def bitstreamPath(child: DynamoTable) =
    s"Representation_Preservation/${child.id}/Generation_1"

  private[nationalarchives] def childFileName(child: DynamoTable) =
    s"${child.id}${child.fileExtension.map(ext => s".$ext").getOrElse("")}"

  private def getAllPaths(path: String): List[String] = {
    def generator(path: String, paths: List[String]): List[String] = {
      paths match {
        case Nil => path :: Nil
        case head :: tail =>
          val newPath = if (path.isEmpty) head else s"$path/$head"
          newPath :: generator(newPath, tail)
      }
    }
    generator("", path.split("/").toList.filter(_.nonEmpty))
  }

  private[nationalarchives] def createOpex(
      asset: DynamoTable,
      children: List[DynamoTable],
      assetXipSize: Long,
      identifiers: List[Identifier],
      securityDescriptor: String = "open"
  ): IO[String] = IO {
    val xml =
      <opex:OPEXMetadata xmlns:opex={opexNamespace}>
        <opex:DescriptiveMetadata>
          <Source xmlns="http://dr2.nationalarchives.gov.uk/source">
            <DigitalAssetSource>{asset.digitalAssetSource}</DigitalAssetSource>
            <DigitalAssetSubtype>{asset.digitalAssetSubtype}</DigitalAssetSubtype>
            <IngestDateTime>{ingestDateTime}</IngestDateTime>
            <OriginalFiles>
              {asset.originalFiles.map(originalFile => <File>{originalFile}</File>)}
            </OriginalFiles>
            <OriginalMetadataFiles>
              {asset.originalMetadataFiles.map(originalMetadataFile => <File>{originalMetadataFile}</File>)}
            </OriginalMetadataFiles>
            <TransferDateTime>{asset.transferCompleteDatetime}</TransferDateTime>
            <TransferringBody>{asset.transferringBody}</TransferringBody>
            <UpstreamSystem>{asset.upstreamSystem}</UpstreamSystem>
            <UpstreamSystemRef>{identifiers.find(_.identifierName == "UpstreamSystemReference").get.value}</UpstreamSystemRef>
          </Source>
        </opex:DescriptiveMetadata>
        <opex:Transfer>
          <opex:Manifest>
            <opex:Folders>
              {
        children
          .map(bitstreamPath)
          .flatMap(path => getAllPaths(path))
          .toSet
          .map((folder: String) => <opex:Folder>{folder}</opex:Folder>)
      }
            </opex:Folders>
            <opex:Files>
              <opex:File type="metadata" size={assetXipSize.toString}>{asset.id}.xip</opex:File>
              {children.map(child => <opex:File type="content" size={child.fileSize.getOrElse(0).toString}>{bitstreamPath(child)}/{childFileName(child)}</opex:File>)}
            </opex:Files>
          </opex:Manifest>
        </opex:Transfer>
        <opex:Properties>
          <opex:Title>{asset.title.getOrElse(asset.name)}</opex:Title>
          <opex:Description>{asset.description.getOrElse("")}</opex:Description>
          <opex:SecurityDescriptor>{securityDescriptor}</opex:SecurityDescriptor>
          {
        if (identifiers.nonEmpty) {
          <opex:Identifiers>
            {identifiers.map(identifier => <opex:Identifier type={identifier.identifierName}>{identifier.value}</opex:Identifier>)}
          </opex:Identifiers>
        }
      }
        </opex:Properties>
      </opex:OPEXMetadata>
    prettyPrinter.format(xml)
  }

  private[nationalarchives] def createXip(asset: DynamoTable, children: List[DynamoTable], securityTag: String = "open"): IO[String] = {
    val xip = <XIP xmlns="http://preservica.com/XIP/v6.4">
      <InformationObject>
        <Ref>{asset.id}</Ref>
        <SecurityTag>{securityTag}</SecurityTag>
        <Title>Preservation</Title>
      </InformationObject>
      <Representation>
        <InformationObject>{asset.id}</InformationObject>
        <Type>Preservation</Type>
        <Name>Preservation</Name>
        <ContentObjects>
          {children.map(child => <ContentObject>{child.id}</ContentObject>)}
        </ContentObjects>
      </Representation>
      {
      children.map { child =>
        <ContentObject>
          <Ref>{child.id}</Ref>
          <Title>{child.name}</Title>
          <Parent>{asset.id}</Parent>
          <SecurityTag>{securityTag}</SecurityTag>
        </ContentObject>
          <Generation original="true" active="true">
            <ContentObject>{child.id}</ContentObject>
            <Bitstreams>
              <Bitstream>{bitstreamPath(child)}/{childFileName(child)}</Bitstream>
            </Bitstreams>
          </Generation>
          <Bitstream>
            <Filename>{childFileName(child)}</Filename>
            <FileSize>{child.fileSize.getOrElse(0)}</FileSize>
            <PhysicalLocation>{bitstreamPath(child)}</PhysicalLocation>
            <Fixities>
              <Fixity>
                <FixityAlgorithmRef>SHA256</FixityAlgorithmRef>
                <FixityValue>{child.checksumSha256.getOrElse("")}</FixityValue>
              </Fixity>
            </Fixities>
          </Bitstream>
      }
    }
  </XIP>
    IO(prettyPrinter.format(xip) + "\n")
  }
}
object XMLCreator {
  // When we can get actual ingest DateTime, we'll retrieve it from the dynamoTable instead
  def apply(ingestDateTime: OffsetDateTime): XMLCreator = new XMLCreator(ingestDateTime)
}
