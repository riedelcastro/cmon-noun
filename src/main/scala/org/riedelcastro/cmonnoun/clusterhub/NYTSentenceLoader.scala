package org.riedelcastro.cmonnoun.clusterhub

import akka.actor._
import akka.actor.Actor._
import java.io._
import nlp.{SentenceDetector, Tokenizer}
import org.xml.sax.SAXException
import javax.xml.parsers.{ParserConfigurationException, DocumentBuilderFactory}
import org.w3c.dom.{Document => XMLDoc}
import com.nytlabs.corpus.NYTCorpusDocumentParser
import org.riedelcastro.cmonnoun.clusterhub.CorpusManager.StoreSentence

/**
 * @author sriedel
 */
object NYTSentenceLoader {


  def parseNYTCorpusDocumentFromFile(file: File) = {
    parser.parseNYTCorpusDocumentFromFile(file, false)
    //    parser.parseNYTCorpusDocumentFromDOMDocument(file, loadNonValidating(file))
  }

  private val parser = new NYTCorpusDocumentParser
  //
  private def parseStringToDOM(s: String, encoding: String, file: File): XMLDoc = {
    try {
      val factory = DocumentBuilderFactory.newInstance()
      factory.setValidating(false)
      val is = new ByteArrayInputStream(s.getBytes(encoding))
      val doc = factory.newDocumentBuilder().parse(is)
      doc
    } catch {
      case e: SAXException => {
        e.printStackTrace()
        sys.error("Exception processing file " + file + ".")
      }
      case e: ParserConfigurationException => {
        e.printStackTrace()
        sys.error("Exception processing file " + file + ".")
      }
      case e: IOException => {
        e.printStackTrace()
        sys.error("Exception processing file " + file + ".")
      }
    }

  }

  private def loadNonValidating(file: File): XMLDoc = {
    var document: XMLDoc = null
    val sb = new StringBuffer()
    try {
      val in = new BufferedReader(new InputStreamReader(
        new FileInputStream(file), "UTF8"))
      var line: String = null
      while ( {(line = in.readLine()); line} != null) {
        sb.append(line + "\n")
      }
      var xmlData = sb.toString;
      xmlData = xmlData.replace("<!DOCTYPE nitf "
        + "SYSTEM \"http://www.nitf.org/"
        + "IPTC/NITF/3.3/specification/dtd/nitf-3-3.dtd\">", "")
      document = parseStringToDOM(xmlData, "UTF-8", file)
      in.close()
      document
    } catch {
      case e: UnsupportedEncodingException => {
        e.printStackTrace()
        System.out.println("Error loading file " + file + ".")
      }
      case e: FileNotFoundException => {
        e.printStackTrace()
        System.out.println("Error loading file " + file + ".")
      }
      case e: IOException => {
        e.printStackTrace()
        System.out.println("Error loading file " + file + ".")
      }
    }
    null
  }

  lazy val tokenizer = new Tokenizer
  lazy val sentenceDetector = new SentenceDetector

  class DocLoaderMaster2(cm:ActorRef) extends SimpleDivideAndConquerActor {
    type BigJob = Files
    type SmallJob = File
    def numberOfWorkers = 10
    def unwrapJob = {case f: Files => f}
    def divide(bigJob: Files) = for (file <- bigJob.files.toIterator) yield file

    def smallJob(file: File) {
      val parsed = parseNYTCorpusDocumentFromFile(file)
      val docId = file.getName
      val sents = sentenceDetector.sentenceDetect(parsed.getBody)
      for ((sent, sentIndex) <- sents.zipWithIndex) {
        val tokens = for ((t, i) <- tokenizer.tokenize(sent.txt).zipWithIndex) yield {
          CorpusManager.Token(i, t.word)
        }
        val sentence = CorpusManager.Sentence(docId, sentIndex, tokens)
        cm ! StoreSentence(sentence)
      }
    }


  }

  case class Files(files: Seq[File])

  def main(args: Array[String]) {
    //    load nyt documents and add these to a corpus
    println("Beginning")
    val cm = actorOf(new CorpusManager("nyt")).start()
    val docLoader = actorOf(new DocLoaderMaster2(cm)).start()
    DivideAndConquerActor.bigJobDoneHook(docLoader){
      () =>
        docLoader.stop()
        cm.stop()
    }
    val files = Seq(new File("/Users/riedelcastro/corpora/nyt/data/2007/01/01/1815718.xml"))
    docLoader ! Files(files)

    //    load freebase entities and add them to an entity collection
    //    val ec = Actor.actorOf[EntityCollectionManager].start()
    //    ec ! EntityCollectionManager.SetCollection("freebase")
    //    how to save mentions?
    //      (a) as token clusters?
    //      (b) as mention objects?
    //    Actor.registry.shutdownAll()
    println("End")


  }

}

object Blah {
  def main(args: Array[String]) {
    println("Test")
  }
}