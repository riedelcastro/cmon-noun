package org.riedelcastro.cmonnoun.clusterhub.nlp

import org.riedelcastro.nurupo.Util
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}

/**
 * @author sriedel
 */
class SentenceDetector {

  val modelIn = Util.getStreamFromFileOrClassPath("en-sent.bin")
  val model = try {
    new SentenceModel(modelIn)
  } catch {
    case e => e.printStackTrace(); sys.error("Can't load model")
  }
  val detector = new SentenceDetectorME(model)

  def sentenceDetect(text:String) = {
    val spans = detector.sentPosDetect(text)
    for (span <- spans) yield {
      Sentence(text.substring(span.getStart, span.getEnd), span.getStart,span.getEnd)
    }
  }

}

object SentenceDetector {

}