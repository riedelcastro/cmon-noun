package org.riedelcastro.cmonnoun.comet

import net.liftweb.http.{SessionVar, CometActor}
import org.riedelcastro.cmonnoun.{StopClustering, StartClustering, Result, ClusterEngine}
import net.liftweb.common.{Full, Box}
import org.riedelcastro.cmonnoun.clusterhub.{Mailbox, ClusterHub}
import akka.actor.{ActorRef, Actor, Actors}

case class ClientState(words: Seq[String])

trait WithBridge {

  protected val bridge = Actors.actorOf(classOf[BridgeActor]).start()
  bridge ! this

  implicit def asAkkaRecipient(to:ActorRef) = new AnyRef {
    def ak_!(msg:Any) {
      to.!(msg)(Some(bridge))
    }
    def ak_!!(msg:Any) = {
      to.!!(msg)(Some(bridge))
    }

  }

  def sendAsAkka(to:ActorRef, msg:Any) {
    to.!(msg)(Some(bridge))
  }


}

/**
 * @author sriedel
 */
class ClusterDisplay extends CometActor with WithBridge {


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
  val clusterHub = Actors.actorOf(classOf[ClusterHub]).start()
  val mailbox = Actors.actorOf(classOf[Mailbox]).start()
}


class BridgeActor extends Actor {
  private var target: Option[CometActor] = None

  def receive = {
    case comet: CometActor => target = Some(comet)
    case msg => target.foreach(_ ! msg)
  }


}