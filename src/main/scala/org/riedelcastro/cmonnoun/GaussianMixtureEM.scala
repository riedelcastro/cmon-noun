package org.riedelcastro.cmonnoun

import java.util.Arrays
import util.Random
import org.riedelcastro.nurupo.HasLogger

/**
 * @author sriedel
 */
object GaussianMixtureEM extends HasLogger {


  def main(args: Array[String]) {
    val D = 2
    val K = 2
    val N = 10
    val spec = new GaussianMixtureSpec(D, K, N)
    spec.gaussians(0).setMean(Seq(1.0,1.0))
    spec.gaussians(0).setCov(Seq(1.0,1.0))
    spec.gaussians(1).setMean(Seq(-1.0,-1.0))
    spec.gaussians(1).setCov(Seq(1.0,1.0))
    spec.prior(0) = 0.7
    spec.prior(1) = 0.3
    spec.maxIteration = 10
    println(spec.prior.mkString(" "))
    spec.gaussians.foreach(println(_))

    spec.sampleObservations()
    spec.randomizeGaussians()
    spec.emAlgorithm()

    println(spec.prior.mkString(" "))
    spec.gaussians.foreach(println(_))

  }

}

class GaussianMixtureSpec(val D: Int, val K: Int, val N: Int) extends HasLogger {
  val gaussians = for (k <- 0 until K) yield new GaussianDistributionDiagonal(D)
  for (gaussian <- gaussians) gaussian.randomize()
  val q = Array.ofDim[Double](N, K)
  val x = Array.ofDim[Double](N, D)
  val prior = Array.ofDim[Double](K)
  val tmp = Array.ofDim[Double](K)
  val N_k = Array.ofDim[Double](K)
  var maxIteration = 3
  var convergenceEps = 0.01
  var logLikelihood = 0.0

  def randomizeGaussians() {
    for (gaussian <- gaussians) gaussian.randomize()
  }

  def sampleObservations() {
    for (i <- 0 until N) {
      val z = sampleMixture()
      val gaussian = gaussians(z)
      gaussian.sample(x(i))
    }
  }

  def sampleMixture():Int = {
    val r = MyRandom.nextDouble()
    var total = 0.0
    var i = 0
    while (i < K) {
      total += prior(i)
      if (r <= total) return i
      i+=1
    }
    K-1
  }

  def eStep() {
    //e-step
    for (i <- 0 until N) {
      var normalizer = 0.0
      for (k <- 0 until K) {
        tmp(k) = prior(k) * gaussians(k).likelihood(x(i))
        normalizer += tmp(k)
      }
      for (k <- 0 until K) {
        q(i)(k) = tmp(k) / normalizer
      }
    }
  }
  def mStep() {
    //m-step
    for (gaussian <- gaussians) gaussian.clear()
    //get k-normalizers
    Arrays.fill(N_k, 0.0)
    for (i <- 0 until N) {
      for (k <- 0 until K) {
        N_k(k) += q(i)(k)
      }
    }
    //get new means
    for (i <- 0 until N) {
      for (k <- 0 until K) {
        for (o <- 0 until D) {
          gaussians(k).mean(o) += (q(i)(k) * x(i)(o)) / N_k(k)
        }
      }
    }
    //get new covariances
    for (i <- 0 until N) {
      for (k <- 0 until K) {
        for (o <- 0 until D) {
          val diff = x(i)(o) - gaussians(k).mean(o)
          gaussians(k).cov(o) += diff * diff * q(i)(k) / N_k(k)
        }
      }
    }
    //get new priors
    for (k <- 0 until K) {
      prior(k) = N_k(k) / N
    }
  }
  def evaluateLogLikelihood() {
    logLikelihood = 0.0
    for (i <- 0 until N) {
      var tmp = 0.0
      for (k <- 0 until K) tmp += prior(k) * gaussians(k).likelihood(x(i))
      logLikelihood += math.log(tmp)
    }
  }
  def emAlgorithm() {
    var iteration = 0
    var oldLL = Double.NegativeInfinity
    var delta = Double.PositiveInfinity

    def terminate() = {
      iteration >= maxIteration || math.abs(delta) < convergenceEps
    }

    def updateDelta() {
      delta = oldLL - logLikelihood
      oldLL = logLikelihood
      logger.info("LL: " + logLikelihood)
    }

    while (!terminate()) {
      eStep()
      mStep()
      evaluateLogLikelihood()
      updateDelta()

      iteration += 1

    }
  }

}


object MyRandom extends Random(0)

class GaussianDistributionDiagonal(dim: Int) {
  val cov = Array.ofDim[Double](dim)
  val mean = Array.ofDim[Double](dim)

  def setMean(mean:Seq[Double]) { for (i <- 0 until dim) this.mean(i) = mean(i)}
  def setCov(cov:Seq[Double]) { for (i <- 0 until dim) this.cov(i) = cov(i)}


  override def toString = {
    val meanString = mean.mkString(" ")
    val covString = cov.mkString(" ")
    "N(%s,%s)".format(meanString,covString)
  }
  def clear() {
    Arrays.fill(mean, 0.0)
    Arrays.fill(cov, 0.0)
  }
  def likelihood(x: Array[Double]) = {
    val det = cov.product
    var exponent = 0.0
    for (i <- 0 until dim) {
      val diff = x(i) - mean(i)
      exponent += -0.5 * diff * 1.0 / cov(i) * diff
    }
    val invDenominator = math.pow(2.0 * math.Pi, dim / 2.0) * math.sqrt(det)
    1.0 / invDenominator * math.exp(exponent)
  }
  def randomize() {
    for (i <- 0 until dim) {
      mean(i) = math.random
      cov(i) = math.random
    }
  }

  def sample(into: Array[Double]) {
    for (i <- 0 until dim) {
      val tmp = MyRandom.nextGaussian()
      into(i) = mean(i) + math.sqrt(cov(i)) * tmp
    }
  }

  def dist(x: Array[Double]) = {
    var result = 0.0
    for (i <- 0 until dim) {
      val diff = x(i) - mean(i)
      result += diff * diff
    }
    result
  }

}

object Test {
  def main(args: Array[String]) {
    println("test yo brother")
  }
}