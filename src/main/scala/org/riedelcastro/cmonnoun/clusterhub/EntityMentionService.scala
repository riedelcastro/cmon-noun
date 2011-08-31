package org.riedelcastro.cmonnoun.clusterhub

import org.riedelcastro.cmonnoun.clusterhub.CorpusService.SentenceSpec
import org.bson.types.ObjectId
import akka.actor.Actor
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._


/**
 * @author sriedel
 */
class EntityMentionService(val collection: String) extends Actor with MongoSupport with StopWhenMailboxEmpty {

  import EntityMentionService._

  def mentionColl(): MongoCollection = {
    collFor("entityMentions", collection)
  }
  def storeMention(entityMention: EntityMention) {
    val coll = mentionColl()
    val dbo = MongoDBObject(
      "_id" -> entityMention.id,
      "phrase" -> entityMention.phrase,
      "docId" -> entityMention.sentence.docId,
      "sent" -> entityMention.sentence.sentenceIndex,
      "from" -> entityMention.from,
      "ner" -> entityMention.ner,
      "to" -> entityMention.to)
    coll += dbo
  }

  private def toEntityMention(dbo: DBObject): EntityMentionService.EntityMention = {
    val id = dbo.as[ObjectId]("_id")
    val docId = dbo.as[String]("docId")
    val sent = dbo.as[Int]("sent")
    val from = dbo.as[Int]("from")
    val to = dbo.as[Int]("to")
    val ner = dbo.getAs[String]("ner")
    val phrase = dbo.as[String]("phrase")
    EntityMention(SentenceSpec(docId, sent), from, to, id = id, ner = ner, phrase = phrase)
  }

  private def query(q: Query) = {
    val coll = mentionColl()
    val dboQ = q.predicate match {
      case All => MongoDBObject()
      case ByIds(ids) => MongoDBObject("_id" -> MongoDBObject("$in" -> ids))
    }
    for (dbo <- coll.find(dboQ).skip(q.skip).limit(q.batchSize)) yield
      toEntityMention(dbo)
  }


  protected def receive = {

    stopWhenMailboxEmpty orElse {
      case StoreEntityMention(m) =>
        storeMention(m)

      case q: Query =>
        val result = query(q)
        self.channel ! EntityMentions(result)
    }

  }
}

object EntityMentionService {
  case class EntityMention(sentence: SentenceSpec, from: Int, to: Int,
                           phrase: String,
                           ner: Option[String] = None,
                           id: ObjectId = new ObjectId)
  case class StoreEntityMention(entityMention: EntityMention)
  case class GetEntityMentions(ids: Stream[Any])
  case class EntityMentions(mentions: TraversableOnce[EntityMention])
  sealed trait Predicate
  case object All extends Predicate
  case class ByIds(ids:Stream[Any]) extends Predicate
  case class Query(predicate: Predicate = All, skip: Int = 0, batchSize: Int = Int.MaxValue)
}

trait MentionManager {

}