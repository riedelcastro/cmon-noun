package org.riedelcastro.cmonnoun.comet

import net.liftweb.http._
import org.riedelcastro.cmonnoun.clusterhub._
import net.liftweb.common.{Empty, Full, Box}
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub._

case class ProblemListDisplayState(names: Seq[String])

/**
 * @author sriedel
 */
class TaskListViewer extends CometActor with WithBridge {

  private var taskNames:Box[Seq[String]] = Empty

  def render = {
    taskNames match {
      case Full(names) => {
        ".problem *" #> names.map(name => {
          ".link *" #> name &
            ".link [href]" #> "problem/%s".format(name)
        })
      }
      case _ => {
        ".problem" #> "Empty"
      }
    }
  }


  override def mediumPriority = {
    case TaskAdded(name,_) => {
      Controller.problemManager ! GetTaskNames
    }
    case TaskNames(names) => {
      taskNames = Full(names)
      reRender()
    }
  }

  override protected def localSetup() {
    Controller.problemManager ! RegisterTaskListener(bridge)
  }

  override protected def localShutdown() {
    Controller.problemManager ! DeregisterTaskListener(bridge)
    bridge.stop()
  }
}