package org.riedelcastro.cmonnoun.clusterhub

import org.riedelcastro.cmonnoun.clusterhub.CorpusManager.SentenceSpec
import org.bson.types.ObjectId
import akka.actor.Actor
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._


/**
 * @author sriedel
 */
class EntityMentionService(val collection: String) extends Actor with MongoSupport {

  import EntityMentionService._

  def storeMention(entityMention: EntityMention) {
    val coll = collFor("entityMentions", collection)
    val dbo = MongoDBObject(
      "_id" -> entityMention.id,
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
    EntityMention(SentenceSpec(docId, sent), from, to, id = id, ner = ner)
  }


  protected def receive = {
    case StoreEntityMention(m) =>
      storeMention(m)

  }
}

object EntityMentionService {
  case class EntityMention(sentence: SentenceSpec, from: Int, to: Int,
                           ner: Option[String] = None,
                           id: ObjectId = new ObjectId)
  case class StoreEntityMention(entityMention: EntityMention)
  case class GetMentions(entityId: ObjectId)
  case class EntityMentions(mentions: TraversableOnce[EntityMention])
}

trait MentionManager {

}