package org.riedelcastro.cmonnoun.clusterhub

/**
 * @author sriedel
 */
trait BinaryNaiveBayesModel extends BinaryClusterEstimation with BinaryClusterMaximization {

  import BinaryClusterService._

  //todo: represent model
  //todo: implement these two methods
  def estimate(instances: Stream[Instance]) = {
    null
  }
  def maximize(instances: Stream[Instance]) {

  }
}