package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{Actor, ActorRef}
import org.riedelcastro.nurupo.HasLogger

/**
 * @author sriedel
 */
object ClusterManager {
  case class SetCluster(id:String,taskName:String, taskManager:ActorRef)
  case object Train
}

class ClusterManager extends Actor with HasLogger with MongoSupport{

  import ClusterManager._

  case class State(clusterId:String, taskManager:ActorRef, taskName:String)

  private var state:Option[State] = None


  protected def receive = {
    case SetCluster(id,n,tm) =>
      state = Some(State(id,tm,n))
      tm ! TaskManager.GetInstances

    case TaskManager.Instances(instances) =>
      //refresh data structures

  }
}