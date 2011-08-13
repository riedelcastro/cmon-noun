package org.riedelcastro.cmonnoun.comet

import org.riedelcastro.cmonnoun.clusterhub._
import org.riedelcastro.cmonnoun.clusterhub.TaskManager._
import akka.actor.ActorRef
import net.liftweb.common.{Full, Empty, Box, Failure}
import xml.Text
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub.{DeregisterTaskListener, RegisterTaskListener, AssignedTaskManager, GetTaskManager}
import org.riedelcastro.cmonnoun.clusterhub.Mailbox.NoSuchMessage
import org.riedelcastro.nurupo.HasLogger
import net.liftweb.http.{SHtml, CometActor}
import net.liftweb.http.js.JsCmds.SetHtml

/**
 * @author sriedel
 */
class TaskViewer extends CometActor with WithBridge with HasLogger {

  private var manager: Box[ActorRef] = Empty
  private var taskName: Box[String] = Empty
  private var instances: Seq[Instance] = Seq.empty

  def render = {
    debugLazy("Rendering now with " + instances.mkString(","))
    val instancesPart = manager match {
      case Full(m) =>

        "#instancesBody *" #> instances.map(i => {
          ".content *" #> i.content
//           <tr><td>{i.content}</td></tr>
        })
      case _ =>
        "#instances" #> Text("No Instances")
    }
    instancesPart
  }


  override def highPriority = {
    case SetTask(n, hub) =>
      taskName = Full(n)
      Controller.clusterHub ak_! GetTaskManager(n)

    case AssignedTaskManager(taskManager) =>
      manager = taskManager
      for (m <- manager) {
        m ak_! RegisterTaskListener(bridge)
      }
      reRender()

    case InstanceAdded(_, instance) =>
      logger.debug(lazyString("Instance added: " + instance))
      for (m <- manager) {
        m ak_! GetInstances
      }

    case Instances(i) =>
      this.instances = i
      logger.debug(lazyString("Current instances: " + instances.mkString(",")))
      reRender()

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