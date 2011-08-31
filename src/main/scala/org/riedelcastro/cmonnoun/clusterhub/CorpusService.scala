package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.Actor
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.Imports._
import org.riedelcastro.cmonnoun.clusterhub.CorpusService.SentenceSpec
import com.mongodb.DBObject


/**
 * @author sriedel
 */

object CorpusService {
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

  sealed trait Predicate
  case object All extends Predicate
  case class BySpecs(specs:Stream[SentenceSpec]) extends Predicate
  case class Query(predicate:Predicate = All, skip:Int = 0, limit:Int = Int.MaxValue)
  case class StoreSentence(sentence: Sentence)
  case class SetCorpus(corpusId: String)
  case class Sentences(sentences: TraversableOnce[Sentence])

  trait SentencesChangedEvent
  case class SentenceAdded(sentence:Sentence) extends SentencesChangedEvent
}

class CorpusService extends Actor with MongoSupport with HasListeners with StopWhenMailboxEmpty {

  import CorpusService._

  private var corpus: Option[String] = None

  def this(corpusId:String) {
    this()
    corpus = Some(corpusId)
  }

  private def spec2Key(spec:SentenceSpec):Any = {
    spec.docId + "#" + spec.sentenceIndex
  }
  private def key2Spec(key:Any):SentenceSpec = {
    val string = key.toString
    val index = string.lastIndexOf("#")
    val docId = string.slice(0,index)
    val sentenceId = string.drop(index).toInt
    SentenceSpec(docId,sentenceId)
  }

  def storeSentence(sentence: Sentence) {
    for (c <- corpus) {
      val coll = collFor("data", c, "sentences")
      val dbo = MongoDBObject("doc" -> sentence.docId, "index" -> sentence.indexInDoc)
      val words = sentence.tokens.map(_.word).toArray
      dbo.put("tokens", words)
      dbo.put("_id", spec2Key(sentence.sentenceSpec))
      coll += dbo
    }
  }

  def querySentences(q: Query): Option[TraversableOnce[Sentence]] = {
    for (c <- corpus) yield {
      val coll = collFor("data", c, "sentences")
      val dboQ = q.predicate match {
        case All => MongoDBObject()
        case BySpecs(specs) => MongoDBObject(
          "_id" -> MongoDBObject("$in" -> specs.map(spec2Key(_))))
      }
      for (dbo <- coll.find(dboQ).skip(q.skip).limit(q.limit)) yield {
        val spec = key2Spec(dbo("_id"))
        val words = dbo.as[BasicDBList]("tokens").toSeq.map(_.toString)
        val tokens = words.zipWithIndex.map({case (w, i) => Token(i, w)})
        Sentence(spec.docId, spec.sentenceIndex, tokens)
      }
    }
  }

  protected def receive = {

    receiveListeners orElse stopWhenMailboxEmpty orElse  {

      case SetCorpus(id) =>
        corpus = Some(id)

      case StoreSentence(s) =>
        storeSentence(s)
        informListeners(SentenceAdded(s))

      case q:Query =>
        for (result <- querySentences(q)) {
          self.reply(Sentences(result))
        }
    }
  }
}