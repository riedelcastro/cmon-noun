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
class EntityMentionManager(val collection: String) extends Actor with MongoSupport {

  import EntityMentionManager._

  def storeMention(entityMention: EntityMention) {
    val coll = collFor("entityMentions", collection)
    val dbo = MongoDBObject(
      "_id" -> entityMention.id,
      "docId" -> entityMention.sentence.docId,
      "sent" -> entityMention.sentence.sentenceIndex,
      "from" -> entityMention.from,
      "to" -> entityMention.to,
      "entityId" -> entityMention.entityId)
    coll += dbo
  }

  private def toEntityMention(dbo: DBObject): EntityMentionManager.EntityMention = {
    val id = dbo.as[ObjectId]("_id")
    val docId = dbo.as[String]("docId")
    val sent = dbo.as[Int]("sent")
    val from = dbo.as[Int]("from")
    val to = dbo.as[Int]("to")
    val entityId = dbo.as[ObjectId]("entityId")
    EntityMention(SentenceSpec(docId, sent), from, to, entityId, id)
  }

  def getMentions(entityId: ObjectId) = {
    val coll = collFor("entityMentions", collection)
    for (dbo <- coll.find(MongoDBObject("entityId" -> entityId))) yield {
      toEntityMention(dbo)
    }
  }


  protected def receive = {
    case StoreEntityMention(m) =>
      storeMention(m)

    case GetMentions(id) =>
      self.reply(EntityMentions(getMentions(id)))
  }
}

object EntityMentionManager {
  case class EntityMention(sentence: SentenceSpec, from: Int, to: Int, entityId: ObjectId, id: ObjectId)
  case class StoreEntityMention(entityMention: EntityMention)
  case class GetMentions(entityId: ObjectId)
  case class EntityMentions(mentions: TraversableOnce[EntityMention])
}

trait MentionManager {

}