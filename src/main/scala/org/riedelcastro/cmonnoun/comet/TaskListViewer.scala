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
        ".task *" #> names.map(name => {
          ".link *" #> name &
            ".link [href]" #> "task/%s".format(name)
        })
      }
      case _ => {
        ".task" #> "Empty"
      }
    }
  }


  override def mediumPriority = {
    case TaskAdded(name,_) => {
      Controller.clusterHub ak_! GetTaskNames
    }
    case TaskNames(names) => {
      taskNames = Full(names)
      reRender()
    }
  }

  override protected def localSetup() {
    Controller.clusterHub ak_! RegisterTaskListener(bridge)
    val answer = Controller.clusterHub ak_!! GetTaskNames
    for (a <- answer) mediumPriority(a)
  }

  override protected def localShutdown() {
    Controller.clusterHub ak_! DeregisterTaskListener(bridge)
    bridge.stop()
  }
}