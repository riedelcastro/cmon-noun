package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.Actor
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.Imports._
import org.riedelcastro.cmonnoun.clusterhub.CorpusManager.SentenceSpec


/**
 * @author sriedel
 */

object CorpusManager {
  case class Token(index: Int, word: String)
  case class TokenPair(i: Token, j: Token)
  case class Sentence(docId: String, indexInDoc: Int, tokens: Seq[Token]) extends Context {
    lazy val sentenceSpec = SentenceSpec(docId,indexInDoc)
    def token(spec:TokenSpec):Option[Token] = {
      if (spec.sentence.docId == docId && spec.sentence.sentenceIndex == indexInDoc)
        tokens.lift(spec.tokenIndex)
      else
        None
    }
  }

  trait InstanceSpec[C <: Context, T] {
    def instance(context: C): Option[T]
  }
  trait Context

  case class TokenSpec(sentence:SentenceSpec, tokenIndex:Int)
  case class SentenceSpec(docId:String, sentenceIndex:Int)

  case class TokenPairSpec(i: Int, j: Int) extends InstanceSpec[Sentence, TokenPair] {
    def instance(context: Sentence) = {
      val lift = context.tokens.lift
      for (ti <- lift(i); tj <- lift(j)) yield TokenPair(ti, tj)
    }
  }

  trait Navigator
  case class OffsetNavigator(offset: Int) extends Navigator

  trait Extractor
  case class RegexExtractor(regex: String)

  case class FeatureSpec(navigator: Navigator, extractor: Extractor)

  case class SentenceQuery(contains: String, from: Int = 0, batchSize: Int = Int.MaxValue)

  case class StoreSentence(sentence: Sentence)

  case class SetCorpus(corpusId: String)

  case class Sentences(sentences: TraversableOnce[Sentence])

  trait SentencesChangedEvent
  case class SentenceAdded(sentence:Sentence) extends SentencesChangedEvent
}

class CorpusManager extends Actor with MongoSupport with HasListeners {

  import CorpusManager._

  private var corpus: Option[String] = None

  def this(corpusId:String) {
    this()
    corpus = Some(corpusId)
  }

  def storeSentence(sentence: Sentence) {
    for (c <- corpus) {
      val coll = collFor("data", c, "sentences")
      val dbo = MongoDBObject("doc" -> sentence.docId, "index" -> sentence.indexInDoc)
      val words = sentence.tokens.map(_.word).toArray
      dbo.put("tokens", words)
      coll += dbo
    }
  }

  def querySentences(q: SentenceQuery): Option[TraversableOnce[Sentence]] = {
    for (c <- corpus) yield {
      val coll = collFor("data", c, "sentences")
      for (dbo <- coll.find().skip(q.from).limit(q.batchSize)) yield {
        val docId = dbo.as[String]("doc")
        val sentenceIndex = dbo.as[Int]("index")
        val words = dbo.as[BasicDBList]("tokens").toSeq.map(_.toString)
        val tokens = words.zipWithIndex.map({case (w, i) => Token(i, w)})
        Sentence(docId, sentenceIndex, tokens)
      }
    }
  }

  protected def receive = {

    receiveListeners.orElse {

      case SetCorpus(id) =>
        corpus = Some(id)

      case StoreSentence(s) =>
        storeSentence(s)
        informListeners(SentenceAdded(s))

      case q@SentenceQuery(w, f, t) =>
        for (result <- querySentences(q)) {
          self.reply(Sentences(result))
        }
    }
  }
}