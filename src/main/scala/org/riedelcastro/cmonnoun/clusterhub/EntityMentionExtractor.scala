package org.riedelcastro.cmonnoun.clusterhub

import org.riedelcastro.nurupo.Util
import opennlp.tools.namefind.{NameFinderME, TokenNameFinderModel}
import org.riedelcastro.cmonnoun.clusterhub.EntityMentionService.EntityMention
import org.riedelcastro.cmonnoun.clusterhub.CorpusService.{SentenceSpec, Sentence}

class EntityMentionModels {

  lazy val modelPersonFile = Util.getStreamFromFileOrClassPath("en-ner-person.bin")
  lazy val modelOrgFile = Util.getStreamFromFileOrClassPath("en-ner-organization.bin")
  lazy val modelLocFile = Util.getStreamFromFileOrClassPath("en-ner-location.bin")

  lazy val personFinderModel = OpenNLPUtil.unsafe(() => new TokenNameFinderModel(modelPersonFile))
  lazy val orgFinderModel = OpenNLPUtil.unsafe(() => new TokenNameFinderModel(modelOrgFile))
  lazy val locFinderModel = OpenNLPUtil.unsafe(() => new TokenNameFinderModel(modelLocFile))

}
/**
 * @author sriedel
 */
class EntityMentionExtractor(val models: EntityMentionModels) {

  import models._

  lazy val personFinder = new NameFinderME(personFinderModel)
  lazy val orgFinder = new NameFinderME(orgFinderModel)
  lazy val locFinder = new NameFinderME(locFinderModel)

  private def mentions(spec: SentenceSpec, array: Array[String], finder: NameFinderME, ner: String) = {
    for (span <- finder.find(array)) yield {
      val phrase = array.slice(span.getStart, span.getEnd).mkString(" ")
      val mention = EntityMention(sentence = spec, from = span.getStart,
        to = span.getEnd, ner = Some(ner), phrase = phrase)
      mention
    }
  }

  def extractMentions(sentence: Sentence): Seq[EntityMention] = {
    extractMentions(Seq(sentence))
  }

  def extractMentions(sentences: TraversableOnce[Sentence]): Seq[EntityMention] = {
    val deep = for (sentence <- sentences) yield {
      val spec = sentence.sentenceSpec
      val array = sentence.tokens.map(_.word).toArray
      val personMentions = mentions(spec, array, personFinder, "PER")
      val locMentions = mentions(spec, array, locFinder, "LOC")
      val orgMentions = mentions(spec, array, orgFinder, "ORG")
      personMentions ++ locMentions ++ orgMentions

    }
    personFinder.clearAdaptiveData()
    locFinder.clearAdaptiveData()
    orgFinder.clearAdaptiveData()

    deep.flatMap(x=>x).toSeq

  }


}

object OpenNLPUtil {
  def unsafe[T](@scala.throws(classOf[Exception]) func: () => T): T = {
    try {
      func()
    } catch {
      case e => sys.error(e.getMessage)
    }
  }
}