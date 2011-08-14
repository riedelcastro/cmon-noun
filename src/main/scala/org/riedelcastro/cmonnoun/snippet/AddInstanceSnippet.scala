package org.riedelcastro.cmonnoun.snippet

import net.liftweb.http.SHtml
import org.riedelcastro.cmonnoun.comet.{Controller, WithBridge}
import net.liftweb.util._
import Helpers._
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub.{AssignedTaskManager, GetTaskManager, CreateTask}
import akka.actor.ActorRef
import org.riedelcastro.nurupo.HasLogger
import net.liftweb.common.{Box, Full, Empty}
import org.riedelcastro.cmonnoun.clusterhub.TaskManager.{AddField, Instances, GetInstances}
import org.riedelcastro.cmonnoun.clusterhub.{Instance, RegExFieldSpec, TaskManager}

/**
 * @author sriedel
 */
class AddInstanceSnippet(taskName: String) extends HasLogger {

  lazy val taskManager = Helper.taskManager(taskName)

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

class AddFieldSpec(taskName: String) extends HasLogger {
  lazy val taskManager = Helper.taskManager(taskName)

  def render = {
    var name: String = "field"
    var regex: String = "*"

    def process() {
      for (m <- taskManager) m ! AddField(RegExFieldSpec(name, regex))
    }
    "name=fieldName" #> SHtml.onSubmit(name = _) & // set the name
      "name=fieldRegex" #> SHtml.onSubmit(regex = _) & // set the name
      "type=submit" #> SHtml.onSubmitUnit(process)


  }
}


class ShowInstances(taskName: String) {
  lazy val taskManager = Helper.taskManager(taskName)
  lazy val instances = for (t <- taskManager;
                            i <- Helper.getInstances(t)) yield i

  def render() = {
    "#instances2 *" #> instances.map(_.map(_.content).mkString(","))
  }

}

object Helper extends HasLogger {

  def getFrom[T](actor: ActorRef, msg: Any)(parse: PartialFunction[Any, T]): Box[T] = {
    val returnMsg = (actor !! msg)
    returnMsg match {
      case Some(x) => parse.lift(x)
      case None => Empty
    }
  }

  implicit def toProxy(actor: ActorRef) = new AnyRef {
    def ??[T](msg: Any)(parse: PartialFunction[Any, T]) = getFrom[T](actor, msg)(parse)
  }

  def test(name: String) = (Controller.clusterHub ?? GetTaskManager(name)) {
    case AssignedTaskManager(Full(m)) => m
  }

  def tm(name: String) = getFrom[ActorRef](Controller.clusterHub, GetTaskManager(name)) {
    case AssignedTaskManager(Full(m)) => m
  }

  def taskManager(name: String): Box[ActorRef] = {
    (Controller.clusterHub !! GetTaskManager(name)) match {
      case Some(AssignedTaskManager(m)) =>
        logger.debug("Received manager " + m)
        m
      case _ =>
        logger.debug("No manager with name" + name)
        Empty
    }
  }

  def getInstances(taskManager: ActorRef) = {
    (taskManager !! GetInstances) match {
      case Some(Instances(i)) =>
        logger.debug("Received instances " + i)
        Full(i)
      case _ =>
        logger.debug("No instances")
        Empty
    }

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