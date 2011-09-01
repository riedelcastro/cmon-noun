package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.Actor
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.BasicDBList
import com.mongodb.casbah.Imports._
import org.riedelcastro.cmonnoun.clusterhub.FeatureService._
import collection.mutable.HashMap


/**
 * @author sriedel
 */
trait FeatureService extends Actor {
  this:FeatureStorage with Vocab =>
  protected def receive = {

    case StoreFeatures(feats) =>
      store(feats)

    case StoreNamedFeatures(feats) =>
      val interned = feats.map(f => Features(f.id, f.features.map(intern(_))))
      store(interned)

    case GetFeatures(ids) =>
      val features = loadFeatures(ids)
      self.channel ! FeatureStream(features)
  }

}

trait Vocab {
  def intern(value:String):Int
}

trait FeatureStorage {

  import FeatureService._

  def store(feats: Stream[Features])
  def loadFeatures(ids: Stream[Any]): Stream[Features]

}
trait MongoVocab extends Vocab {
  val store = new HashMap[String,Int]
  def intern(value: String) = {
    store.getOrElseUpdate(value, store.size)
  }
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
  def store(feats: Stream[Features]) {
    for (feat <- feats) {
      val dbo = MongoDBObject(
        "_id" -> feat.id,
        "feats" -> feat.features.toArray
      )
      coll += dbo
    }
  }
}

class BasicFeatureService(val name:String) extends FeatureService with MongoFeatureStorage with MongoVocab {
}

object FeatureService {
  case class Features(id: Any, features: Set[Int])
  case class NamedFeatures(id: Any, features: Set[String])
  case class FeatureStream(featureStream: Stream[Features])
  case class StoreFeatures(featureStream: Stream[Features])
  case class StoreNamedFeatures(featureStream: Stream[NamedFeatures])

  case class GetFeatures(ids: Stream[Any])
}
