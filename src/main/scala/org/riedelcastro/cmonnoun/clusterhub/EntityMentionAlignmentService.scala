package org.riedelcastro.cmonnoun.clusterhub

import org.bson.types.ObjectId
import org.riedelcastro.cmonnoun.clusterhub.EntityService.ByIds
import org.riedelcastro.cmonnoun.clusterhub.EntityMentionAlignmentService.{EntityIds, GetEntityIds, GetEntities, StoreAlignment}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.Imports._
import akka.actor.{ScalaActorRef, ActorRef, Actor}


/**
 * @author sriedel
 */
class EntityMentionAlignmentService(id:String) extends Actor with MongoSupport {

  val coll = collFor("entityMentionAlign",id)

  def entityIdsFor(mentionId:ObjectId):TraversableOnce[String] = {
    for (dbo <- coll.find(MongoDBObject("mention" -> mentionId))) yield {
      dbo.as[String]("entity")
    }
  }

  def storeAlignment(mentionId:ObjectId,entityId:String) {
    val dbo = MongoDBObject(
      "mention" -> mentionId,
      "entity" -> entityId)
    coll += dbo
  }



  protected def receive = {
    case StoreAlignment(mentionId,entityId) =>
      storeAlignment(mentionId,entityId)

    case GetEntities(m,mentionId) =>
      val entityIds = entityIdsFor(mentionId)
      m.forward(EntityService.Query(ByIds(entityIds.toSeq)))

    case GetEntityIds(mentionId) =>
      self.channel ! EntityIds(entityIdsFor(mentionId))

  }
}

object EntityMentionAlignmentService {
  case class StoreAlignment(mentionId:ObjectId,entityId:String)
  case class GetEntityIds(mentionId:ObjectId)
  case class GetEntities(entityService:ScalaActorRef, mentionId:ObjectId)
  case class GetMentionIds(entityIds:Stream[Any])

  case class EntityIds(entityId:TraversableOnce[String])
}