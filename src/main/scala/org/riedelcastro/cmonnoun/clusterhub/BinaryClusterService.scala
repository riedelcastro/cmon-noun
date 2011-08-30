package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.Actor
import org.riedelcastro.cmonnoun.clusterhub.BinaryClusterService._
import collection.mutable.HashMap

/**
 * @author sriedel
 */
trait BinaryClusterService
  extends Actor
  with HasListeners
  with BinaryClusterStorage
  with BinaryClusterEstimation
  with BinaryClusterMaximization {

  protected def receive = {

    receiveListeners orElse {

      case SetLabels(labels) =>
        storeLabels(labels)

      case SetPenalties(penalties) =>
        storePenalties(penalties)

      case SetFeatures(features) =>
        storeFeatures(features)

      case Estimate(ids) =>
        val instances = loadInstances(ids)
        val probs = estimate(instances)
        storeProbabilities(probs)
        informListeners(ProbabilitiesChanged(probs))

      case Maximize(ids) =>
        val instances = loadInstances(ids)
        maximize(instances)
        informListeners(ModelChanged)

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
  case object ModelChanged
  case class ProbabilitiesChanged(ids:Stream[Probability])

}

trait InMemoryBinaryClusterStorage extends BinaryClusterStorage {

  import BinaryClusterService._

  private val storage = new HashMap[Any,Instance]

  def loadInstances(ids: Stream[Any]) = for (id <- ids; instance <- storage.get(id)) yield instance

  def replace(id:Any,subs:Instance=>Instance) {
    storage(id) = storage.getOrElse(id, subs(defaultInstance(id)))
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


trait BinaryClusterEstimation {

  import BinaryClusterService._

  def estimate(instances: Stream[Instance]): Stream[Probability]

}

trait BinaryClusterMaximization {

  import BinaryClusterService._

  def maximize(instances:Stream[Instance])
}


object BinaryClusterService {

  case class Label(id: Any, label: Double)
  case class Probability(id: Any, prob: Double)
  case class Penalty(id: Any, penalty: Double)
  case class Features(id: Any, features: Set[Int])

  def defaultInstance(id:Any) = Instance(id,0.5,0.0,None,Set.empty)

  case class Instance(id: Any, prob: Double, penalty: Double = 0.0, label: Option[Double] = None, feats: Set[Int])
  case class SetLabels(labels: Stream[Label])
  case class SetPenalties(penalties: Stream[Penalty])
  case class SetFeatures(features: Stream[Features])

  case class Estimate(ids: Stream[Any])
  case class Maximize(ids: Stream[Any])

}

