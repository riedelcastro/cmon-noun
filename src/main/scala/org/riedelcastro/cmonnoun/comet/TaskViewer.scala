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
import org.riedelcastro.cmonnoun.snippet.CometInitializer
import org.riedelcastro.nurupo.HasLogger

/**
 * @author sriedel
 */
class TaskViewer extends KnowsTask with CallMailboxFirst {

  private var instances: Seq[Instance] = Seq.empty

  def cometType = "task"

  def render = {
    debugLazy("Rendering now with " + instances.mkString(","))
    val instancesPart = manager match {
      case Full(m) =>
        "#instancesBody *" #> instances.map(i => {
          ".content *" #> i.content &
            ".field *" #> i.fields.map(_._2.toString)
        })
      case _ =>
        "#instances" #> Text("No Instances")
    }
    instancesPart
  }

  override protected def assignTaskManager(m: ActorRef) {
    m ak_! RegisterTaskListener(bridge)
    m ak_! GetInstances

  }

  override protected def taskChanged() {
    for (m <- manager) {
      m ak_! GetInstances
    }
  }

  override def lowPriority = super.lowPriority orElse {

    case Instances(i) =>
      this.instances = i
      logger.debug(lazyString("Current instances: " + instances.mkString(",")))
      reRender()

    case NoSuchMessage(r) =>
      logger.info("No message for ".format(r))


  }

  override protected def localShutdown() {
    super.localShutdown()
    for (m <- manager) m ak_! DeregisterTaskListener(bridge)
  }

}

trait CallMailboxFirst extends CometActor with WithBridge {

  def cometType: String

  override protected def localSetup() {
    for (n <- name) {
      (Controller.mailbox ak_!! Mailbox.RetrieveMessage(n)) match {
        case Some(msg) => messageHandler(msg)
        case _ =>
      }

    }
  }

  override protected def localShutdown() {
    bridge.stop()
  }
}

trait KnowsTask extends CometActor with WithBridge with HasLogger {

  protected var manager: Box[ActorRef] = Empty
  protected var taskName: Box[String] = Empty

  protected def assignTaskManager(m: ActorRef) {
    m ak_! RegisterTaskListener(bridge)
    m ak_! GetInstances

  }

  protected def taskChanged() {}

  override def lowPriority = {
      case SetTask(n, hub) =>
      taskName = Full(n)
      Controller.clusterHub ak_! GetTaskManager(n)

    case AssignedTaskManager(taskManager) =>
      manager = taskManager
      for (m <- manager) assignTaskManager(m)
      reRender()

    case change: TaskChanged =>
      logger.debug(lazyString("Task Changed"))
      taskChanged()


  }
}

class ClusterListViewer extends CallMailboxFirst with KnowsTask {
  def cometType = "clusterList"
  def render = {
    "#cluster *" #> Seq(".name" #> "A", ".name" #> "B")
  }
}