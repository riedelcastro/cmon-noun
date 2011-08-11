package org.riedelcastro.cmonnoun

import akka.actor.{ActorRef, Actor}
import util.Random
import org.riedelcastro.nurupo.HasLogger

case class StartClustering(display: ActorRef)

case object StopClustering

case class Result(words: Seq[String])

/**
 * @author sriedel
 */
class ClusterEngine extends Actor with HasLogger {
  val words = Seq("officer", "worker", "actor", "spiegel", "department")
  var stop = false

  protected def receive = {
    case msg@StartClustering(display) => {
      logger.info("Received: " + msg)
      while (!stop) {
        Thread.sleep(1000)
        display ! Result(Shuffle.shuffle(words))
        logger.info("Result sent")
      }
      logger.info("Stopped clustering")
    }
    case msg@StopClustering => {
      logger.info("Received: " + msg)
      stop = true
    }

  }
}

object Shuffle {
  def shuffle[T<:AnyRef](seq: Seq[T]): Seq[T] = {
    val rnd = new Random
    val array = seq.indices.toArray
    for (n <- Iterator.range(array.length - 1, 0, -1)) {
      val k = rnd.nextInt(n + 1)
      val t = array(k); array(k) = array(n); array(n) = t
    }
    seq.indices.map(i => seq(array(i)))
  }
}