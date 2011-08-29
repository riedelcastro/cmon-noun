package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.Actor
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.MongoCollection
import org.riedelcastro.cmonnoun.clusterhub.EntityManager._
import com.mongodb.casbah.Imports._


/**
 * @author sriedel
 */
object EntityManager {

  case class Entity(id: String, name: String,
                    features: Seq[String] = Seq.empty,
                    freebaseTypes:Seq[String] = Seq.empty)
  case class Entities(entities: TraversableOnce[Entity])
  case class AddEntity(entity: Entity)
  case class SetCollection(id: String)
  case class Query(predicate: Predicate, skip: Int, batchSize: Int)
  sealed trait Predicate
  case class ById(id: String) extends Predicate
  case object All extends Predicate

}

trait EntityCollectionPersistence extends MongoSupport {
  this: EntityManager =>

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

class EntityManager(val collectionId: String) extends Actor with HasListeners with EntityCollectionPersistence {


  protected def receive = {
    receiveListeners orElse {

      case AddEntity(entity) =>
        addEntity(entity)

      case q: Query =>
        self.channel ! Entities(query(q))
    }
  }
}