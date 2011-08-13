package org.riedelcastro.cmonnoun.comet

import net.liftweb.http.CometActor
import org.riedelcastro.cmonnoun.clusterhub._
import org.riedelcastro.cmonnoun.clusterhub.TaskManager._
import akka.actor.ActorRef
import net.liftweb.common.{Full, Empty, Box, Failure}
import xml.Text
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub.{DeregisterTaskListener, RegisterTaskListener, AssignedTaskManager, GetTaskManager}
import org.riedelcastro.cmonnoun.clusterhub.Mailbox.NoSuchMessage
import org.riedelcastro.nurupo.HasLogger

/**
 * @author sriedel
 */
class TaskViewer extends CometActor with WithBridge with HasLogger {

  private var manager:Box[ActorRef] = Empty
  private var taskName:Box[String] = Empty

  def render = {
    val namePart = taskName match {
      case Full(n) => "#name *" #> n
      case _ =>  "#name" #> Text("No Name given")
    }
    namePart
  }


  override def mediumPriority = {
    case SetTask(n,hub) =>
      taskName = Full(n)
      Controller.clusterHub ak_! GetTaskManager(n)

    case AssignedTaskManager(taskManager) =>
      manager = taskManager
      for (m <- manager) m ak_! RegisterTaskListener(bridge)
      reRender(false)

    case NoSuchMessage(r) =>
      logger.info("No message for ".format(r))

  }

  override protected def localSetup() {
    Controller.mailbox ak_! Mailbox.RetrieveMessage(name.getOrElse("NoName"))
  }

  override protected def localShutdown() {
    for (m <- manager) m ak_! DeregisterTaskListener(bridge)
    bridge.stop()
  }


}