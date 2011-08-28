package org.riedelcastro.cmonnoun.clusterhub

import akka.actor._
import akka.actor.Actor._
import java.io.File
import akka.routing.{CyclicIterator, Routing}
import java.io._
import org.xml.sax.SAXException
import javax.xml.parsers.{ParserConfigurationException, DocumentBuilderFactory}
import org.w3c.dom.{Document => XMLDoc}
import com.nytlabs.corpus.NYTCorpusDocumentParser
import cc.refectorie.proj.factorieie.data.KnowledgeBase
import cc.refectorie.proj.factorieie.annotator.{CoreNLPTagger, CoreNLPSentenceSplitter, CoreNLPTokenizer}
import org.riedelcastro.cmonnoun.clusterhub.CorpusManager.StoreSentence

/**
 * @author sriedel
 */
object NYTFreebase {


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

  case class DocTask(file: File)
  case object Done
  case object Start

  val kb = new KnowledgeBase()

  class NYTDocLoader(val corpus: ActorRef) extends Actor {
    protected def receive = {
      case DocTask(file) =>
        val parsed = parseNYTCorpusDocumentFromFile(file)
        val docId = file.getName
        val doc = kb.createDocument(docId, parsed.getBody)
        CoreNLPTokenizer.annotate(doc)
        CoreNLPSentenceSplitter.annotate(doc)
//        CoreNLPTagger.annotate(doc)
        for (sent <- doc.sentences) {
          val tokens = sent.tokens.map(t => CorpusManager.Token(t.indexInSentence, t.word))
          val sentence = CorpusManager.Sentence(docId, sent.indexInDocument, tokens)
          corpus ! StoreSentence(sentence)
        }
        self.reply(Done)
    }
  }

  class DocLoaderMaster extends Actor {
    val cm = actorOf[CorpusManager].start()
    val docLoaders = List.fill(10)(actorOf(new NYTDocLoader(cm)).start())
    val router = Routing.loadBalancerActor(new CyclicIterator(docLoaders)).start()

    cm ! CorpusManager.SetCorpus("nyt")
    val files = Seq(new File("/Users/riedelcastro/corpora/nyt/data/2007/01/01/1815718.xml"))

    protected def receive = {
      case Start =>
        for (file <- files) {
          router ! DocTask(file)
        }
        self.reply(Done)
    }
  }

  def main(args: Array[String]) {
    //load nyt documents and add these to a corpus
    val docLoader = actorOf[DocLoaderMaster].start()
    docLoader !! Start

    //load freebase entities and add them to an entity collection
//    val ec = Actor.actorOf[EntityCollectionManager].start()
//    ec ! EntityCollectionManager.SetCollection("freebase")
    //how to save mentions?
    //  (a) as token clusters?
    //  (b) as mention objects?

  }

}