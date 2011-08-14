package org.riedelcastro.cmonnoun.comet

import net.liftweb.http.CometActor
import org.riedelcastro.cmonnoun.clusterhub.{Mailbox, ClusterHub}
import akka.actor.{ActorRef, Actor, Actors}

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

object Controller {
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