package org.riedelcastro.cmonnoun.clusterhub

import akka.actor._
import akka.actor.Actor._
import java.io._
import nlp.{SentenceDetector, Tokenizer}
import org.xml.sax.SAXException
import javax.xml.parsers.{ParserConfigurationException, DocumentBuilderFactory}
import org.w3c.dom.{Document => XMLDoc}
import com.nytlabs.corpus.NYTCorpusDocumentParser
import org.riedelcastro.cmonnoun.clusterhub.CorpusService.StoreSentence
import java.util.concurrent.TimeUnit
import org.riedelcastro.nurupo.Util
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import opennlp.tools.tokenize.TokenizerModel
import xsbti.AppConfiguration

/**
 * @author sriedel
 */
class NYTSentenceLoaderLauncher extends xsbti.AppMain {

  def run(app: AppConfiguration) = {
    NYTSentenceLoader.main(app.arguments())
    new xsbti.Exit {val code = 0}
  }

}

object NYTSentenceLoader {


  def parseNYTCorpusDocumentFromFile(file: File, parser:NYTCorpusDocumentParser) = {
//    parser.parseNYTCorpusDocumentFromFile(file, false)
    val doc = loadNonValidating(file)
    parser.parseNYTCorpusDocumentFromDOMDocument(file, doc)
  }

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
      case e => {
        e.printStackTrace()
        sys.error("Error loading file " + file + ".")
      }
    }
  }

  class DocLoaderMaster(cm: ActorRef) extends DivideAndConquerActor {
    type BigJob = Files
    type SmallJob = File

    def numberOfWorkers = 10

    def unwrapJob = {case f: Files => f}

    def divide(bigJob: Files) = for (file <- bigJob.files.toIterator) yield file

    def newWorker() = new DocSlave

    lazy val modelIn = Util.getStreamFromFileOrClassPath("en-sent.bin")
    lazy val model = OpenNLPUtil.unsafe(() => new SentenceModel(modelIn))

    lazy val tokModelIn = Util.getStreamFromFileOrClassPath("en-token.bin")
    lazy val tokModel: TokenizerModel = OpenNLPUtil.unsafe(() => new TokenizerModel(tokModelIn))
    lazy val nytParser = new NYTCorpusDocumentParser


    class DocSlave extends Worker {
      val detector = new SentenceDetector(model)
      val tokenizer = new Tokenizer(tokModel)

      def doYourJob(file: File) {
        try {
          val parsed = parseNYTCorpusDocumentFromFile(file,nytParser)
          val docId = file.getName
          if (parsed.getBody != null) {
            val sents = detector.sentenceDetect(parsed.getBody)
            for ((sent, sentIndex) <- sents.zipWithIndex) {
              val tokens = for ((t, i) <- tokenizer.tokenize(sent.txt).zipWithIndex) yield {
                CorpusService.Token(i, t.word)
              }
              val sentence = CorpusService.Sentence(docId, sentIndex, tokens)
              cm ! StoreSentence(sentence)
            }
          }
        } catch {
          case e =>
            e.printStackTrace()
            warnLazy(e.getMessage)
        }
      }

    }


  }

  case class Files(files: Seq[File])

  def main(args: Array[String]) {
    //    load nyt documents and add these to a corpus
    val corpusId = NeoConf.get("nyt-mongo", "nyt")
    val cm = actorOf(new CorpusService(corpusId)).start()
    val docLoader = actorOf(new DocLoaderMaster(cm)).start()
    DivideAndConquerActor.bigJobDoneHook(docLoader) {
      () =>
        docLoader.stop()
        Scheduler.schedule(cm, StopWhenMailboxEmpty, 0, 1, TimeUnit.SECONDS)
    }
    val nytPrefix = NeoConf.get[File]("nyt-prefix")
    val files = if (nytPrefix.isDirectory) Util.files(nytPrefix)
    else {
      val dir = nytPrefix.getParentFile
      val prefix = nytPrefix.getName
      Util.files(dir).filter(_.getName.startsWith(prefix))
    }
    docLoader ! Files(files)

  }

}

