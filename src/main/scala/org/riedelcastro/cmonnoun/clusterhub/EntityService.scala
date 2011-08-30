package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.Actor
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.MongoCollection
import org.riedelcastro.cmonnoun.clusterhub.EntityService._
import com.mongodb.casbah.Imports._


/**
 * @author sriedel
 */
object EntityService {

  case class Entity(id: String, name: String,
                    features: Seq[String] = Seq.empty,
                    freebaseTypes:Seq[String] = Seq.empty)
  case class Entities(entities: TraversableOnce[Entity])
  case class AddEntity(entity: Entity)
  case class EntityAdded(entity:Entity)
  case class SetCollection(id: String)
  case class Query(predicate: Predicate, skip: Int = 0, batchSize: Int = Int.MaxValue)
  sealed trait Predicate
  case class ById(id: String) extends Predicate
  case class ByIds(ids: Seq[String]) extends Predicate
  case class ByName(name:String) extends Predicate

  case object All extends Predicate

}

trait EntityCollectionPersistence extends MongoSupport {
  this: EntityService =>

  def entityColl(id: String): MongoCollection = {
    collFor(id, "entities")
  }

  def addEntity(entity: Entity) {
    val coll = entityColl(collectionId)
    val dbo = MongoDBObject(
      "_id" -> entity.id,
      "name" -> entity.name,
      "feats" -> entity.features,
      "freebaseTypes" -> entity.freebaseTypes
    )
    coll += dbo
  }

  def query(query: Query) = {
    val coll = entityColl(collectionId)
    val q = query.predicate match {
      case ById(entityId) => MongoDBObject("_id" -> entityId)
      case ByIds(ids) => MongoDBObject("_id" -> MongoDBObject("$in" -> ids))
      case ByName(name) => MongoDBObject("name" -> name)
      case All => MongoDBObject()
    }
    val result = for (dbo <- coll.find(q).skip(query.skip).limit(query.batchSize)) yield {
      val id = dbo.as[String]("_id")
      val name = dbo.as[String]("name")
      val feats = dbo.as[BasicDBList]("feats")
      val types = dbo.as[BasicDBList]("freebaseTypes")
      Entity(id, name, feats.map(_.toString),types.map(_.toString))
    }
    result
  }

}

class EntityService(val collectionId: String)
  extends Actor with HasListeners with EntityCollectionPersistence with StopWhenMailboxEmpty {


  protected def receive = {
    receiveListeners orElse {

      case AddEntity(entity) =>
        addEntity(entity)
        informListeners(EntityAdded(entity))

      case q: Query =>
        self.channel ! Entities(query(q))
    }
  }
}