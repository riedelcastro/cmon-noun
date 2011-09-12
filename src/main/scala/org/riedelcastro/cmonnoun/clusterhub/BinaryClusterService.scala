package org.riedelcastro.cmonnoun.clusterhub

import org.riedelcastro.cmonnoun.clusterhub.BinaryClusterService._
import collection.mutable.HashMap
import akka.actor.{ActorRef, Actor}
import com.mongodb.casbah.Imports._

/**
 * @author sriedel
 */
trait BinaryClusterService extends Actor with HasListeners {
  this: BinaryClusterStorage
    with BinaryClusterEstimation
    with BinaryClusterMaximization
    with BinaryClusterInitialization =>

  def name:String

  protected def receive = {

    receiveListeners orElse {

      case SetLabels(labels) =>
        storeLabels(labels)
        self.channel ! Done

      case SetPenalties(penalties) =>
        storePenalties(penalties)
        self.channel ! Done


      case SetFeatures(features) =>
        storeFeatures(features)
        self.channel ! Done

      case Estimate(ids) =>
        val instances = loadInstances(ids)
        val probs = estimate(instances)
        storeProbabilities(probs)
        informListeners(ProbabilitiesChanged(self, probs))
        self.channel ! Done

      case Initialize(ids) =>
        val instances = loadInstances(ids)
        initialize(instances)
        informListeners(ModelChanged(self))
        self.channel ! Done

      case Maximize(ids) =>
        val instances = loadInstances(ids)
        maximize(instances)
        informListeners(ModelChanged(self))
        self.channel ! Done

      case GetInstances(ids) =>
        val instances = loadInstances(ids)
        self.channel ! Instances(instances)

      case DoEM(ids,iterations) =>
        val instances = loadInstances(ids)
        var probs:Stream[Probability] = Stream.empty
        initialize(instances)
        for (i <- 0 until iterations){
          probs = estimate(instances)
          maximize(instances)
        }
        storeProbabilities(probs)
        informListeners(ProbabilitiesChanged(self, probs))
        informListeners(ModelChanged(self))
        self.channel ! Done

    }
  }
}

trait BinaryClusterStorage {

  import BinaryClusterService._

  def storeProbabilities(probabilities: Stream[Probability])
  def storeLabels(labels: Stream[Label])
  def storePenalties(penalties: Stream[Penalty])
  def storeFeatures(features: Stream[Features])

  def loadInstances(ids: Stream[Any]): Stream[Instance]

}

trait InMemoryBinaryClusterStorage extends BinaryClusterStorage {

  import BinaryClusterService._

  private val storage = new HashMap[Any, Instance]

  def loadInstances(ids: Stream[Any]) = for (id <- ids; instance <- storage.get(id)) yield instance

  def replace(id: Any, subs: Instance => Instance) {
    storage(id) = subs(storage.getOrElse(id, defaultInstance(id)))
  }

  def storeFeatures(features: Stream[Features]) {
    features.foreach(feat => replace(feat.id, _.copy(feats = feat.features)))
  }

  def storeLabels(labels: Stream[Label]) {
    labels.foreach(l => replace(l.id, _.copy(label = Some(l.label))))
  }
  def storePenalties(penalties: Stream[Penalty]) {
    penalties.foreach(p => replace(p.id, _.copy(penalty = p.penalty)))
  }
  def storeProbabilities(probabilities: Stream[Probability]) {
    probabilities.foreach(p => replace(p.id, _.copy(prob = p.prob)))
  }
}

trait MongoBinaryClusterStorage extends BinaryClusterStorage with MongoSupport {

  this: BinaryClusterService =>

  lazy val coll = collFor("cluster",name)
  def loadInstances(ids: Stream[Any]) = {
    val q = MongoDBObject("_id" -> MongoDBObject("$in" -> ids))
    val result = for (dbo <- coll.find(q)) yield {
      val id = dbo("_id")
      val prob = dbo.getAs[Double]("prob").getOrElse(0.5)
      val label = dbo.getAs[Double]("label")
      val feats = dbo.getAs[BasicDBList]("feats").map(_.toSeq)
      val penalty = dbo.as[Double]("penalty")
      BinaryClusterService.Instance(id,prob,penalty,label,feats.getOrElse(Seq.empty).map(_.asInstanceOf[Int]).toSet)
    }
    result.toStream
  }

  private def storeField[T](data:Seq[T],field:String, id:T=>Any, value:T=>Any) {
    for (d <- data) {
      val q = MongoDBObject("_id" -> id(d))
      val u = MongoDBObject("$set" -> MongoDBObject("feats" -> value(d)))
      coll.update(q,u,true,false)
    }
  }

  private def storeListField[T](data:Seq[T],field:String, id:T=>Any, value:T=>Any) {
    for (d <- data) {
      val q = MongoDBObject("_id" -> id(d))
      val u = MongoDBObject("$pushAll" -> MongoDBObject("feats" -> value(d)))
      coll.update(q,u,true,false)
    }
  }


  private def storeOptionalField[T](data:Seq[T],field:String, id:T=>Any, value:T=>Option[Any]) {
    for (d <- data; v <- value(d)) {
      val q = MongoDBObject("_id" -> id(d))
      val u = MongoDBObject("$set" -> MongoDBObject("feats" -> v))
      coll.update(q,u,true,false)
    }
  }


  def storeFeatures(features: Stream[BinaryClusterService.Features]) {
    storeListField[BinaryClusterService.Features](features,"feats",_.id, _.features.toArray)
  }
  def storeLabels(labels: Stream[Label]) {
    storeField[BinaryClusterService.Label](labels,"label",_.id, _.label)
  }
  def storePenalties(penalties: Stream[Penalty]) {
    storeField[BinaryClusterService.Penalty](penalties,"penalty",_.id, _.penalty)
  }
  def storeProbabilities(probabilities: Stream[Probability]) {
    storeField[BinaryClusterService.Probability](probabilities,"prob",_.id, _.prob)
  }
}


trait BinaryClusterEstimation {

  import BinaryClusterService._

  def estimate(instances: Stream[Instance]): Stream[Probability]

}

trait BinaryClusterMaximization {

  import BinaryClusterService._

  def maximize(instances: Stream[Instance])
}

trait BinaryClusterInitialization {

  import BinaryClusterService._

  def initialize(instances: Stream[Instance])
}

object BinaryClusterService {

  case object Done

  case class Label(id: Any, label: Double)
  case class Probability(id: Any, prob: Double)
  case class Penalty(id: Any, penalty: Double)
  type Features = FeatureService.Features

  def defaultInstance(id: Any) = Instance(id, 0.5, 0.0, None, Set.empty)

  case class Instance(id: Any, prob: Double, penalty: Double = 0.0, label: Option[Double] = None, feats: Set[Int])
  case class SetLabels(labels: Stream[Label])
  case class SetPenalties(penalties: Stream[Penalty])
  case class SetFeatures(features: Stream[Features])

  case class Estimate(ids: Stream[Any])
  case class Maximize(ids: Stream[Any])
  case class Initialize(ids: Stream[Any])
  case class DoEM(ids:Stream[Any], iterations:Int)

  case class GetInstances(ids: Stream[Any])

  case class Instances(instances:Stream[Instance])

  case class ModelChanged(cluster: ActorRef)
  case class ProbabilitiesChanged(cluster: ActorRef, ids: Stream[Probability])



  sealed trait Task
  case object EntityClustering extends Task
  case object RelationClustering extends Task


}

class BasicBinaryClusterService(val name:String)
  extends BinaryClusterService with MongoBinaryClusterStorage with BinaryNaiveBayesModel

class BinaryClusterServiceRegistry extends ServiceRegistry {
  def create(name: String) = new BasicBinaryClusterService(name)
  def registryName = "cluster"
}

class BinaryClusterHelper(val clusterService: ActorRef) extends Actor {

  protected def receive = {
    case FeatureService.FeaturesStored(feats) =>
      clusterService ! BinaryClusterService.SetFeatures(feats)
  }
}

object BinaryClusterTest {

  import Actor._

  def main(args: Array[String]) {
    val featureService = actorOf(new BasicFeatureService("entities")).start()
    val clusterService = actorOf(new BasicBinaryClusterService("person")).start()
    val entityService = actorOf(new EntityService("freebase")).start()

    for (FeatureService.FeatureStream(feats) <- featureService !! FeatureService.GetAllFeatures) {
      //set features
      clusterService !! BinaryClusterService.SetFeatures(feats)

      //get entity ids
      val ids = feats.map(_.id)

      //get entities
      for (EntityService.Entities(entities) <- entityService !! EntityService.Query(EntityService.ByIds(ids))) {

        //get labels
        val labels = for (entity <- entities) yield {
          val label = if (entity.freebaseTypes.contains("/people/person")) 1.0 else 0.0
          BinaryClusterService.Label(entity.id,label)
        }

        //set labels
        clusterService !! BinaryClusterService.SetLabels(labels.toStream)
      }

      //run EM
      clusterService !! BinaryClusterService.DoEM(ids,1)

      //print instances
      for (Instances(instances) <- clusterService !! BinaryClusterService.GetInstances(ids)) {
        println(instances.mkString("\n"))
      }

    }
    featureService.stop()
    clusterService.stop()
    entityService.stop()

  }
}
