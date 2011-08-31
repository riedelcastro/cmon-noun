package org.riedelcastro.cmonnoun.clusterhub.nlp

import org.riedelcastro.nurupo.Util
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import org.riedelcastro.cmonnoun.clusterhub.OpenNLPUtil

/**
 * @author sriedel
 */
class SentenceDetector(val model:SentenceModel) {

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