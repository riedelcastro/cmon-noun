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


  class Gaussian(var mean: Double = 0.0, var variance: Double = 1.0) {
    def prob(value: Double) = {
      val denominator = math.sqrt(2.0 * math.Pi * variance)
      val delta = mean - value
      val exponent = -delta * delta / (2.0 * variance)
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
      for ((spec, _) <- gaussiansTrue) {
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

  def evaluateLogLikelihood():Double = {
    var result = 0.0
    for (row <- loadRows()){
      val target = row.label.target
      for (extractor <- extractors) {
        val value = row.instance.fields(extractor.spec.name)
        result += 1.0
      }
    }
    result
  }

  def mStep() {

    val countsTrue = new Sigma(0.0)
    val countsFalse = new Sigma(0.0)
    val gaussStatsTrue = new GaussianParameters()
    val gaussStatsFalse = new GaussianParameters()
    var count = 0
    var totalTrue = 0.0
    for (row <- loadRows()) {
      val prob = row.label.prob
      val probUse = if (row.label.edit != 0.5) row.label.edit else prob
      totalTrue += probUse
      for (extractor <- extractors) {
        extractor.spec.realValued match {
          case false =>
            if (true == row.instance.fields(extractor.spec.name)) {
              countsTrue(extractor.spec) = countsTrue(extractor.spec) + probUse
              countsFalse(extractor.spec) = countsFalse(extractor.spec) + 1.0 - probUse
            }
          case true =>
            val score = row.instance.fields(extractor.spec.name).asInstanceOf[Double]
            gaussStatsTrue.getOrElseUpdate(extractor.spec, new Gaussian).mean += probUse * score
            gaussStatsFalse.getOrElseUpdate(extractor.spec, new Gaussian).mean += (1.0 - probUse) * score

        }
      }
      count += 1
    }
    //normalize means
    for (extractor <- extractors; if (extractor.spec.realValued)) {
      gaussiansTrue(extractor.spec) = new Gaussian(gaussStatsTrue(extractor.spec).mean / totalTrue, 0.0)
      gaussiansFalse(extractor.spec) = new Gaussian(gaussStatsFalse(extractor.spec).mean / (1.0 - totalTrue), 0.0)
    }

    //now calculate variances
    if (extractors.exists(_.spec.realValued)) for (row <- loadRows()) {
      val prob = row.label.prob
      val probUse = if (row.label.edit != 0.5) row.label.edit else prob
      for (extractor <- extractors; if (extractor.spec.realValued)) {
        val score = row.instance.fields(extractor.spec.name).asInstanceOf[Double]
        val diffTrue = gaussiansTrue(extractor.spec).mean - score
        val diffFalse = gaussiansFalse(extractor.spec).mean - score
        gaussiansTrue(extractor.spec).variance += diffTrue * diffTrue * probUse / totalTrue
        gaussiansFalse(extractor.spec).variance += diffFalse * diffFalse * probUse / (1.0 - totalTrue)
      }
    }

    //now update parameters
    prior = totalTrue / count
    for ((k, v) <- countsTrue) {
      sigmaTrue(k) = v / totalTrue
    }
    for ((k, v) <- countsFalse) {
      sigmaFalse(k) = v / (1.0 - totalTrue)
    }


  }

  def prob(row: Row): Double = 1.0
}
























