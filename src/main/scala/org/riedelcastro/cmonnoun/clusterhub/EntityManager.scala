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

  case class Entity(id:String, name:String, features:Seq[String])
  case class Entities(entities:TraversableOnce[Entity])
  case class AddEntity(entity:Entity)
  case class SetCollection(id:String)
  case class Query(predicate:Predicate, skip:Int, batchSize:Int)
  sealed trait Predicate
  case class ById(id:String) extends Predicate
  case object All extends Predicate

}

trait EntityCollectionPersistence extends MongoSupport {
  this: EntityManager =>

  def entityColl(id: String): MongoCollection = {
    collFor(id, "entities")
  }
  def addEntity(entity:Entity) {
    for (id <- collectionId){
      val coll = entityColl(id)
      val dbo = MongoDBObject(
        "_id" -> entity.id,
        "name" -> entity.name,
        "feats" -> entity.features
      )
      coll += dbo
    }
  }

  def query(query:Query) = {
    val ents = for (id <- collectionId) yield {
      val coll = entityColl(id)
      val q = query.predicate match {
        case ById(entityId) => MongoDBObject("_id" -> entityId)
        case All => MongoDBObject()
      }
      val result = for (dbo <- coll.find(q).skip(query.skip).limit(query.batchSize)) yield {
        val id = dbo.as[String]("_id")
        val name = dbo.as[String]("name")
        val feats = dbo.as[BasicDBList]("feats")
        Entity(id,name,feats.map(_.toString))
      }
      result
    }
    ents
  }

}

class EntityManager extends Actor with HasListeners with EntityCollectionPersistence {

  var collectionId:Option[String] = None

  protected def receive = {
    receiveListeners orElse  {
      case SetCollection(id) =>
        collectionId = Some(id)

      case AddEntity(entity)=>
        addEntity(entity)

      case q:Query =>
        for (result <- query(q)) {
          self.reply(Entities(result))
        }
    }
  }
}