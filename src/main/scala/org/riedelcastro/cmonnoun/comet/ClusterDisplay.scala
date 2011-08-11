package org.riedelcastro.cmonnoun.comet

import net.liftweb.http.{SessionVar, CometActor}
import akka.actor.{Actor, Actors}
import org.riedelcastro.cmonnoun.{StopClustering, StartClustering, Result, ClusterEngine}
import net.liftweb.common.{Full, Empty, Box}

case class ClientState(words: Seq[String])

/**
 * @author sriedel
 */
class ClusterDisplay extends CometActor {

  private val bridge = Actors.actorOf(classOf[BridgeActor]).start()
  bridge ! this

  object currentState extends SessionVar[Box[ClientState]](Full(ClientState(Seq.empty)))


  override protected def localSetup() {
    Controller.engine ! StartClustering(bridge)
  }


  override protected def localShutdown() {
    Controller.engine ! StopClustering
    bridge.stop()
  }

  def render = currentState.is match {
    case Full(ClientState(words)) => {
      "#status *" #> "Yo" &
        ".word *" #> words
    }
    case _ => {
      "#status *" #> "Yo" &
        ".word *" #> "test"
    }
  }


  override def lowPriority = {
    case Result(words) => {
      currentState.set(Full(ClientState(words)))
      reRender()
    }
  }
}

object Controller {
  val engine = Actors.actorOf(classOf[ClusterEngine]).start()
}

class BridgeActor extends Actor {
  private var target: Option[CometActor] = None

  def receive = {
    case comet: CometActor => target = Some(comet)
    case msg => target.foreach(_ ! msg)
  }
}