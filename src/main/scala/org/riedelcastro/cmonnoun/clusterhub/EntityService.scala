package org.riedelcastro.cmonnoun.clusterhub

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.MongoCollection
import org.riedelcastro.cmonnoun.clusterhub.EntityService._
import com.mongodb.casbah.Imports._
import collection.mutable.HashMap
import akka.actor.{ActorRef, Actor}

/**
 * @author sriedel
 */
object EntityService {

  case class Entity(id: String, name: String,
                    freebaseTypes:Seq[String] = Seq.empty)
  case class Entities(entities: TraversableOnce[Entity])
  case class AddEntity(entity: Entity)
  case class EntityAdded(entity:Entity)
  case class SetCollection(id: String)
  case class Query(predicate: Predicate, skip: Int = 0, batchSize: Int = Int.MaxValue)
  sealed trait Predicate
  case class ById(id: Any) extends Predicate
  case class ByIds(ids: Seq[Any]) extends Predicate
  case class ByName(name:String) extends Predicate
  case class ByNameRegex(regex:String) extends Predicate
  case class ByIdRegex(regex:String) extends Predicate


  case object All extends Predicate

}

trait EntityCollectionPersistence extends MongoSupport {
  this: EntityService =>

  def entityColl(id: String): MongoCollection = {
    val coll = collFor(id, "entities")
    coll.ensureIndex("name")
    coll
  }

  def addEntity(entity: Entity) {
    val coll = entityColl(collectionId)
    val dbo = MongoDBObject(
      "_id" -> entity.id,
      "name" -> entity.name,
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
      case ByNameRegex(regex) => MongoDBObject("name" -> MongoDBObject("$regex" -> regex))
      case ByIdRegex(regex) => MongoDBObject("_id" -> MongoDBObject("$regex" -> regex))
      case All => MongoDBObject()
    }
    val result = for (dbo <- coll.find(q).skip(query.skip).limit(query.batchSize)) yield {
      val id = dbo.as[String]("_id")
      val name = dbo.as[String]("name")
      val types = dbo.as[BasicDBList]("freebaseTypes")
      Entity(id, name, types.map(_.toString))
    }
    result
  }

}

class EntityService(val collectionId: String)
  extends Actor with HasListeners with EntityCollectionPersistence with StopWhenMailboxEmpty {


  protected def receive = {
    receiveListeners orElse stopWhenMailboxEmpty orElse {

      case AddEntity(entity) =>
        addEntity(entity)
        informListeners(EntityAdded(entity))

      case q: Query =>
        self.channel ! Entities(query(q))
    }
  }
}


class EntityServiceRegistry extends ServiceRegistry {
  def registryName = "entity"
  def create(name: String) = new EntityService(name)
}

class EntityMentionAlignmentServiceRegistry extends ServiceRegistry {
  def create(name: String) = new EntityMentionAlignmentService(name)
  def registryName = "entityMentionAlignment"
}

