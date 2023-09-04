package uk.gov.nationalarchives

import cats.effect.IO
import uk.gov.nationalarchives.Lambda.DynamoTable

import scala.xml._

class XMLCreator {
  private val prettyPrinter = new PrettyPrinter(200, 2)
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
      securityDescriptor: String = "open"
  ): IO[String] = IO {
    val xml =
      <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
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
          <opex:Title>{asset.name}</opex:Title>
          <opex:Description>{asset.description}</opex:Description>
          <opex:SecurityDescriptor>{securityDescriptor}</opex:SecurityDescriptor>
        </opex:Properties>
      </opex:OPEXMetadata>
    prettyPrinter.format(xml)
  }

  private[nationalarchives] def createXip(asset: DynamoTable, children: List[DynamoTable], securityTag: String = "open"): IO[String] = {
    val xip = <XIP xmlns="http://preservica.com/XIP/v6.4">
      <InformationObject>
        <Ref>{asset.id}</Ref>
        <SecurityTag>{securityTag}</SecurityTag>
        <Title/>
      </InformationObject>
      <Representation>
        <InformationObject>{asset.id}</InformationObject>
        <Type>Preservation</Type>
        <Name>Preservation</Name>
        <ContentObjects>
          {children.map(child => <ContentObject>{child.id}</ContentObject>)}
        </ContentObjects>
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
      </Representation>
    </XIP>
    IO(prettyPrinter.format(xip))
  }
}
object XMLCreator {
  def apply(): XMLCreator = new XMLCreator()
}
