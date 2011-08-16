package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{Actor, ActorRef}
import org.riedelcastro.nurupo.HasLogger
import org.bson.types.ObjectId
import collection.mutable.HashMap


/**
 * @author sriedel
 */
trait ProbabilisticModel {

  this: ClusterManager =>

  var prior = 0.5

  class Sigma(init: Double = 0.5) extends HashMap[FieldSpec, Double]() {
    override def default(key: FieldSpec) = init
  }

  class GaussianParameters(default: Gaussian = new Gaussian()) extends HashMap[FieldSpec, Gaussian]() {
    override def default(key: FieldSpec) = default
  }


  class Gaussian {
    var mean = 0.0
    var variance = 1.0
    def prob(value:Double) = {
      val denominator = math.sqrt(2.0 * math.Pi * variance)
      val delta = mean - value
      val exponent = - delta * delta / (2.0 * variance)
      1.0 / denominator * math.exp(exponent)
    }
  }


  val gaussiansTrue = new GaussianParameters
  val gaussiansFalse = new GaussianParameters

  val sigmaTrue = new Sigma(0.5)
  val sigmaFalse = new Sigma(0.5)

  def eStep() {
    for (row <- loadRows()) {
      var likelihoodTrue = prior
      var likelihoodFalse = 1.0 - prior
      for ((spec, _) <- sigmaTrue) {
        val pTrue = sigmaTrue(spec)
        val pFalse = sigmaFalse(spec)
        val value = true == row.instance.fields(spec.name)
        likelihoodTrue *= (if (value) pTrue else 1.0 - pTrue)
        likelihoodFalse *= (if (value) pFalse else 1.0 - pFalse)
      }
      for ((spec,_) <- gaussiansTrue){
        val gaussianTrue = gaussiansTrue(spec)
        val gaussianFalse = gaussiansFalse(spec)
        val value = row.instance.fields(spec.name).asInstanceOf[Double]
        likelihoodTrue *= gaussianTrue.prob(value)
        likelihoodFalse *= gaussianFalse.prob(value)
      }

      val normalizer = likelihoodTrue + likelihoodFalse
      val prob = likelihoodTrue / normalizer
      setProb(row.id, prob)
    }
  }

  def mStep() {

    val countsTrue = new Sigma(0.0)
    val countsFalse = new Sigma(0.0)
    var count = 0
    var totalTrue = 0.0
    for (row <- loadRows()) {
      val prob = row.label.prob
      val probUse = if (row.label.edit != 0.5) row.label.edit else prob
      totalTrue += probUse
      for (spec <- extractors) {
        if (true == row.instance.fields(spec.spec.name)) {
          countsTrue(spec.spec) = countsTrue(spec.spec) + probUse
          countsFalse(spec.spec) = countsFalse(spec.spec) + 1.0 - probUse
        }
      }
      count += 1
    }
    prior = totalTrue / count
    for ((k,v) <- countsTrue) {
      sigmaTrue(k) = v / totalTrue
    }
    for ((k,v) <- countsFalse) {
      sigmaFalse(k) = v / (1.0 - totalTrue)
    }


  }

  def prob(row: Row): Double = 1.0
}
























