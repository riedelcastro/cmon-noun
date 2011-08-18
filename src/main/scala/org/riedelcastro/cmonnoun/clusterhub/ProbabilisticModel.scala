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

  trait Distribution {
    def prob(value: Any): Double
  }

  class Binomial(var prob: Double = 0.0) extends Distribution {
    def prob(value: Any) = if (true == value) prob else 1.0 - prob
  }


  class Gaussian(var mean: Double = 0.0, var variance: Double = 1.0) extends Distribution {
    def prob(any: Any) = {
      val value = any.asInstanceOf[Double]
      val denominator = math.sqrt(2.0 * math.Pi * variance)
      val delta = mean - value
      val exponent = -delta * delta / (2.0 * variance)
      1.0 / denominator * math.exp(exponent)
    }
  }

  class Distributions extends HashMap[FieldSpec, Distribution] {
    override def default(key: FieldSpec) = if (key.realValued) new Gaussian() else new Binomial()
    def gaussian(key: FieldSpec, mean: Double = 0.0, variance: Double = 1.0) =
      getOrElseUpdate(key, new Gaussian(mean, variance)).asInstanceOf[Gaussian]

    def binomial(key: FieldSpec, init: Double = 0.5) =
      getOrElseUpdate(key, new Binomial(init)).asInstanceOf[Binomial]

    def binomialMap() = filterKeys(!_.realValued).mapValues(_.asInstanceOf[Binomial].prob).toMap
    def gaussianMap() = filterKeys(_.realValued).mapValues(v => {
      val g = v.asInstanceOf[Gaussian]
      g.mean -> g.variance
    }).toMap
  }


  val distributionsTrue = new Distributions
  val distributionsFalse = new Distributions

  def resetModel() {
    distributionsTrue.clear()
    distributionsFalse.clear()
    prior = 0.5
  }

  def probForLabel(row: Row): Double = {
    var likelihoodTrue = prior
    var likelihoodFalse = 1.0 - prior
    for ((spec, _) <- distributionsTrue) {
      val fieldValue = row.instance.fields(spec.name)
      val pTrue = distributionsTrue(spec).prob(fieldValue)
      val pFalse = distributionsFalse(spec).prob(fieldValue)
      likelihoodTrue *= pTrue
      likelihoodFalse *= pFalse
    }

    val normalizer = likelihoodTrue + likelihoodFalse
    val prob = likelihoodTrue / normalizer
    prob
  }

  def eStep() {
    for (row <- loadRows()) {
      val prob: Double = probForLabel(row)
      setProb(row.id, prob)
    }
  }

  def evaluateLogLikelihood(): Double = {
    var result = 0.0
    for (row <- loadRows()) {
      val target = row.label.target
      for (extractor <- extractors) {
        val value = row.instance.fields(extractor.spec.name)
        result += 1.0
      }
    }
    result
  }

  def mStep() {

    val statsTrue = new Distributions()
    val statsFalse = new Distributions()
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
              statsTrue.binomial(extractor.spec, 0.0).prob += probUse
              statsFalse.binomial(extractor.spec, 0.0).prob += 1.0 - probUse
            }
          case true =>
            val score = row.instance.fields(extractor.spec.name).asInstanceOf[Double]
            statsTrue.gaussian(extractor.spec, 0, 0).mean += probUse * score
            statsFalse.gaussian(extractor.spec, 0, 0).mean += (1.0 - probUse) * score
        }
      }
      count += 1
    }
    //normalize means, init variances
    for (extractor <- extractors;
         if (extractor.spec.realValued)) {
      distributionsTrue.gaussian(extractor.spec).variance = 0.0
      distributionsTrue.gaussian(extractor.spec).mean = statsTrue.gaussian(extractor.spec).mean / totalTrue
      distributionsFalse.gaussian(extractor.spec).variance = 0.0
      distributionsFalse.gaussian(extractor.spec).mean = statsFalse.gaussian(extractor.spec).mean / (count - totalTrue)
    }

    //now calculate variances
    if (extractors.exists(_.spec.realValued)) for (row <- loadRows()) {
      val prob = row.label.prob
      val probUse = if (row.label.edit != 0.5) row.label.edit else prob
      for (extractor <- extractors;
           if (extractor.spec.realValued)) {
        val score = row.instance.fields(extractor.spec.name).asInstanceOf[Double]
        val diffTrue = distributionsTrue.gaussian(extractor.spec).mean - score
        val diffFalse = distributionsFalse.gaussian(extractor.spec).mean - score
        distributionsTrue.gaussian(extractor.spec).variance += diffTrue * diffTrue * probUse / totalTrue
        distributionsFalse.gaussian(extractor.spec).variance += diffFalse * diffFalse * (1.0 - probUse) / (count - totalTrue)
      }
    }

    //now update parameters
    prior = totalTrue / count
    for ((k, _) <- statsTrue; if (!k.realValued)) {
      distributionsTrue.binomial(k).prob = statsTrue.binomial(k).prob / totalTrue
    }
    for ((k, _) <- statsFalse; if (!k.realValued)) {
      distributionsFalse.binomial(k).prob = statsFalse.binomial(k).prob / (count - totalTrue)
    }


  }

  def prob(row: Row): Double = 1.0
}
























