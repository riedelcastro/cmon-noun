package org.riedelcastro.cmonnoun.snippet

import net.liftweb.http.SHtml
import org.riedelcastro.cmonnoun.comet.{Controller, WithBridge}
import net.liftweb.util._
import Helpers._
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub.{AssignedTaskManager, GetTaskManager, CreateTask}
import akka.actor.ActorRef
import org.riedelcastro.cmonnoun.clusterhub.TaskManager
import net.liftweb.common.{Full, Empty, Box}
import org.riedelcastro.nurupo.HasLogger

/**
 * @author sriedel
 */
class AddInstance(taskName: String) extends HasLogger {

  lazy val taskManager:Box[ActorRef] = (Controller.clusterHub !! GetTaskManager(taskName)) match {
    case Some(AssignedTaskManager(m)) =>
      logger.debug("Received manager " + m)
      m
    case _ =>
      logger.debug("No manager with name" + taskName)
      Empty
  }

  def render = {
    var content: String = "Content"

    def process() {
      logger.debug("Sending AddInstance of " + content)
      for (m <- taskManager) m ! TaskManager.AddInstance(content)
    }
    "name=content" #> SHtml.onSubmit(content = _) & // set the name
      "type=submit" #> SHtml.onSubmitUnit(process)

  }

}

class AddTask {

  def render() = {
    var taskName: String = "New Task"
    def process() {
      Controller.clusterHub ! CreateTask(taskName)
    }
    "name=taskName" #> SHtml.onSubmit(taskName = _) & // set the name
      "type=submit" #> SHtml.onSubmitUnit(process)
  }

}