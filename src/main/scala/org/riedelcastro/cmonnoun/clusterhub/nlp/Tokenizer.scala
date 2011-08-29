package org.riedelcastro.cmonnoun.clusterhub.nlp

import org.riedelcastro.nurupo.Util
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}

/**
 * @author sriedel
 */
class Tokenizer {

  val modelIn = Util.getStreamFromFileOrClassPath("en-token.bin")
  val model:TokenizerModel = try {
    new TokenizerModel(modelIn)
  } catch {
    case e => e.printStackTrace(); sys.error("Couldn't load model")
  }
  val tokenizer = new TokenizerME(model)

  def tokenize(text: String): Seq[Token] = {
    val result = tokenizer.tokenizePos(text)
    for (span <- result) yield {
      Token(text.substring(span.getStart, span.getEnd), span.getStart, span.getEnd)
    }
  }

}
