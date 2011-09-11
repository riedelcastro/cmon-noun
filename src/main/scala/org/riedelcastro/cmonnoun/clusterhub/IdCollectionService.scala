package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.Actor
import org.riedelcastro.cmonnoun.clusterhub.IdCollectionService.{Ids, StoreIds, GetIds}
import com.mongodb.casbah.commons.MongoDBObject

/**
 * @author sriedel
 */
class IdCollectionService(val name: String) extends Actor with MongoSupport {

  val coll = collFor("collection", name)

  def loadIds() = {
    val result = for (dbo <- coll.find()) yield {
      dbo.get("_id")
    }
    result.toStream
  }

  def storeIds(ids: Seq[Any]) {
    for (id <- ids) {
      coll += MongoDBObject("_id" -> id)
    }
  }

  protected def receive = {
    case GetIds =>
      val ids = loadIds()
      self.channel ! Ids(ids)

    case StoreIds(ids) =>
      storeIds(ids)
  }
}

object IdCollectionService {
  case class Ids(ids: Stream[Any])
  case class StoreIds(ids: Stream[Any])
  case object GetIds
}