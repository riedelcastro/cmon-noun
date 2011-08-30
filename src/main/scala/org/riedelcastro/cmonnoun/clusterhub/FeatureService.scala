package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.Actor
import org.riedelcastro.cmonnoun.clusterhub.FeatureService.{Features, FeatureStream, GetFeatures, StoreFeatures}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.BasicDBList
import com.mongodb.casbah.Imports._


/**
 * @author sriedel
 */
trait FeatureService extends Actor with FeatureStorage {
  protected def receive = {
    case StoreFeatures(feats) =>
      store(feats)
    case GetFeatures(ids) =>
      val features = loadFeatures(ids)
      self.channel ! FeatureStream(features)
  }

}

trait FeatureStorage {

  import FeatureService._

  def store(feats: Stream[Features])
  def loadFeatures(ids: Stream[Any]): Stream[Features]
}

trait MongoFeatureStorage extends FeatureStorage with MongoSupport {

  def name: String

  val coll = collFor("features", name)

  def loadFeatures(ids: Stream[Any]) = {
    val q = MongoDBObject("_id" -> MongoDBObject("$in" -> ids.toArray))
    coll.find(q).map(dbo => {
      val id = dbo.as[Any]("_id")
      val feats = dbo.as[BasicDBList]("feats").map(_.asInstanceOf[Int]).toSet
      Features(id, feats)
    }).toStream
  }
  def store(feats: Stream[Features]) = {
    for (feat <- feats) {
      val dbo = MongoDBObject(
        "_id" -> feat.id,
        "feats" -> feat.features.toArray
      )
      coll += dbo
    }
  }
}

object FeatureService {
  case class Features(id: Any, features: Set[Int])
  case class FeatureStream(featureStream: Stream[Features])
  case class StoreFeatures(featureStream: Stream[Features])
  case class GetFeatures(ids: Stream[Any])
}
