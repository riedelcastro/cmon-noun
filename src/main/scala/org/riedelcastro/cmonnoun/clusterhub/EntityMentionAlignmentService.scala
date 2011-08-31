package org.riedelcastro.cmonnoun.clusterhub

import org.bson.types.ObjectId
import org.riedelcastro.cmonnoun.clusterhub.EntityService.ByIds
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.Imports._
import akka.actor.{ScalaActorRef, Actor}


/**
 * @author sriedel
 */
class EntityMentionAlignmentService(id: String) extends Actor with MongoSupport with StopWhenMailboxEmpty {

  import EntityMentionAlignmentService._

  lazy val coll = collFor("entityMentionAlign", id)
  coll.ensureIndex("entity")
  coll.ensureIndex("mention")


  def entityIdsFor(mentionId: Any): TraversableOnce[Any] = {
    for (dbo <- coll.find(MongoDBObject("mention" -> mentionId))) yield {
      dbo.as[String]("entity")
    }
  }

  def mentionIdsFor(entityId: Any): TraversableOnce[Any] = {
    for (dbo <- coll.find(MongoDBObject("entity" -> entityId))) yield {
      dbo.as[Any]("mention")
    }
  }

  def mentionsIdsFor(entityIds: Stream[Any]): TraversableOnce[Any] = {
    for (dbo <- coll.find(MongoDBObject("entity" -> MongoDBObject("$in" -> entityIds)))) yield {
      dbo.as[Any]("mention")
    }
  }



  def storeAlignment(mentionId: Any, entityId: Any) {
    val dbo = MongoDBObject(
      "mention" -> mentionId,
      "entity" -> entityId)
    coll += dbo
  }


  protected def receive = {
    stopWhenMailboxEmpty orElse {
      case StoreAlignment(mentionId, entityId) =>
        storeAlignment(mentionId, entityId)

      case GetEntities(m, mentionId) =>
        val entityIds = entityIdsFor(mentionId)
        m.forward(EntityService.Query(ByIds(entityIds.toSeq)))

      case GetEntityIds(mentionId) =>
        self.channel ! EntityIds(entityIdsFor(mentionId))

      case GetMentionIds(entityIds) =>
        self.channel ! MentionIds(mentionsIdsFor(entityIds))

      case GetMentions(m,entityIds) =>
        val mentionIds = mentionsIdsFor(entityIds)
        m.forward(EntityMentionService.Query(EntityMentionService.ByIds(mentionIds.toStream)))
    }

  }
}

object EntityMentionAlignmentService {
  case class StoreAlignment(mentionId: Any, entityId: Any)
  case class GetEntityIds(mentionId: Any)
  case class GetEntities(entityService: ScalaActorRef, mentionId: Any)
  case class GetMentionIds(entityIds: Stream[Any])
  case class GetMentions(mentionService: ScalaActorRef, entityIds: Stream[Any])


  case class EntityIds(entityIds: TraversableOnce[Any])
  case class MentionIds(mentionIds:TraversableOnce[Any])
}