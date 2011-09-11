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

  def entities(): TraversableOnce[Any] = {
    for (dbo <- coll.find()) yield {
      dbo.as[String]("entity")
    }
  }


  def mentionIdsFor(entityId: Any): TraversableOnce[Any] = {
    for (dbo <- coll.find(MongoDBObject("entity" -> entityId))) yield {
      dbo.as[Any]("mention")
    }
  }

  def mentionsIdsFor(entityIds: Stream[Any]): TraversableOnce[Alignment] = {
    for (dbo <- coll.find(MongoDBObject("entity" -> MongoDBObject("$in" -> entityIds)))) yield {
      val m = dbo.as[Any]("mention")
      val e = dbo.as[Any]("entity")
      Alignment(m,e)
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
      case StoreAlignment(alignment) =>
        storeAlignment(alignment.mentionId, alignment.entityId)

      case GetEntities(m, mentionId) =>
        val entityIds = entityIdsFor(mentionId)
        m.forward(EntityService.Query(ByIds(entityIds.toSeq)))

      case GetEntitiesWithMentions =>
        self.channel ! EntityIds(entities())

      case GetEntityIds(mentionId) =>
        self.channel ! EntityIds(entityIdsFor(mentionId))

      case GetAlignments(entityIds) =>
        self.channel ! Alignments(mentionsIdsFor(entityIds).map(a=>a.mentionId -> a.entityId).toMap)

      case GetMentions(m,entityIds) =>
        val mentionIds = mentionsIdsFor(entityIds)
        m.forward(EntityMentionService.Query(EntityMentionService.ByIds(mentionIds.toStream)))
    }

  }
}

object EntityMentionAlignmentService {

  type EntityId = Any
  type EntityMentionId = Any

  case class Alignment(mentionId: Any, entityId: Any)
  case class Alignments(alignments:Map[EntityMentionId,EntityId])
  case class StoreAlignment(alignment:Alignment)
  case class GetEntityIds(mentionId: Any)
  case class GetEntities(entityService: ScalaActorRef, mentionId: Any)
  case class GetAlignments(entityIds: Stream[Any])
  case class GetMentions(mentionService: ScalaActorRef, entityIds: Stream[Any])
  case object GetEntitiesWithMentions


  case class EntityIds(entityIds: TraversableOnce[Any])
  case class MentionIds(mentionIds:TraversableOnce[Any])
}