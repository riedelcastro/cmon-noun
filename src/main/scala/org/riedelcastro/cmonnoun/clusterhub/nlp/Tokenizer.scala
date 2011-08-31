package org.riedelcastro.cmonnoun.clusterhub.nlp

import org.riedelcastro.nurupo.Util
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}
import org.riedelcastro.cmonnoun.clusterhub.OpenNLPUtil

/**
 * @author sriedel
 */
class Tokenizer(val model:TokenizerModel) {

  val modelIn = Util.getStreamFromFileOrClassPath("en-token.bin")
  val tokenizer = new TokenizerME(model)

  def tokenize(text: String): Seq[Token] = {
    val result = tokenizer.tokenizePos(text)
    for (span <- result) yield {
      Token(text.substring(span.getStart, span.getEnd), span.getStart, span.getEnd)
    }
  }

}
