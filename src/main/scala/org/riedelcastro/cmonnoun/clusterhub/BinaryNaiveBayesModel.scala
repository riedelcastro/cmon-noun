package org.riedelcastro.cmonnoun.clusterhub

import collection.mutable.HashMap

/**
 * @author sriedel
 */

class ParamVector(defaultValue: Double = 0.0) extends HashMap[Any, Double] {
  override def default(key: Any) = defaultValue

  def inc(key: Any, value: Double): Double = {
    val newValue = apply(key) + value
    update(key, newValue)
    newValue
  }

  def add(that: ParamVector, scale: Double) {
    if (scale != 0.0)
      for ((key, value) <- that) {
        inc(key, value * scale)
      }
  }

  def dot(that: ParamVector): Double = {
    var result = 0.0
    val (vec1, vec2) = if (this.size < that.size) (this, that) else (that, this)
    for ((key, value) <- vec1) {
      val thatValue = vec2(key)
      result += value * thatValue
    }
    result
  }

  def mult(scale: Double) {
    for ((key, value) <- this) {
      update(key, value * scale)
    }
  }

  def regularize(scale: Double) {
    if (scale != 0.0) for ((key, value) <- this) {
      inc(key, value * scale)
    }
  }
}

class DirichletMultinomial(var lambda: Double = 1.0) {
  protected val countVec = new ParamVector()
  protected var sum = 0.0

  def prob(key: Any) = {
    (countVec(key) + lambda) / (sum + lambda * countVec.size)
  }

  def logprob(key: Any) = {
    math.log(countVec(key) + lambda) - math.log(sum + lambda * countVec.size)
  }

  def inc(key: Any, value: Double) {
    countVec.inc(key, value)
    sum += value
  }
}

trait BinaryNaiveBayesModel extends BinaryClusterEstimation with BinaryClusterMaximization with BinaryClusterInitialization {

  import BinaryClusterService._

  //todo: represent model
  var prior = new DirichletMultinomial()
  var emissions0 = new DirichletMultinomial()
  var emissions1 = new DirichletMultinomial()

  //todo: implement these two methods

  def initialize(instances: Stream[Instance]) {
    prior.inc(true, 0)
    prior.inc(false, 99)
    instances.foreach(instance => {
      if (instance.label.isDefined) {
        if (instance.label.get == 1) {
          prior.inc(true, 1)
          instance.feats.foreach(f => {
            emissions1.inc(f, 1)
            emissions0.inc(f, 0)
          })
        } else {
          prior.inc(false, 1)
          instance.feats.foreach(f => {
            emissions1.inc(f, 0)
            emissions0.inc(f, 1)
          })
        }
      } else {
        // needed to ensure we don't ignore unlabeled features
        instance.feats.foreach(f => {
          emissions1.inc(f, 0)
          emissions0.inc(f, 0)
        })
      }
    })
  }

  def estimate(instances: Stream[Instance]) = {
    instances.map(instance => {
      var logprob0 = prior.logprob(false)
      var logprob1 = prior.logprob(true) - instance.penalty
      instance.feats.foreach(f => {
        logprob0 += emissions0.logprob(f)
        logprob1 += emissions1.logprob(f)
      })
      val prob = 1.0 / (1 + math.exp(logprob0 - logprob1))
      Probability(instance.id, prob)
    })
  }

  def maximize(instances: Stream[Instance]) {
    prior = new DirichletMultinomial()
    emissions0 = new DirichletMultinomial()
    emissions1 = new DirichletMultinomial()
    instances.foreach(instance => {
      val prob = {
        if (instance.label.isDefined) {
          if (instance.label.get == 1) 1 else 0
        } else instance.prob
      }
      prior.inc(true, prob)
      prior.inc(false, 1 - prob)
      instance.feats.foreach(f => {
        emissions1.inc(f, prob)
        emissions0.inc(f, 1 - prob)
      })
    })
  }
}