package org.riedelcastro.cmonnoun.comet

import net.liftweb.http._
import org.riedelcastro.cmonnoun.clusterhub._
import net.liftweb.common.{Empty, Full, Box}
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub._
import org.riedelcastro.nurupo.HasLogger
import xml.NodeSeq
import net.liftweb.util.{CssBindImpl, CssSel, CssBind}

case class ProblemListDisplayState(names: Seq[String])

/**
 * @author sriedel
 */
class TaskListViewer extends CometActor with WithBridge with HasLogger {

  private var taskNames: Box[Seq[String]] = Empty


  def render = {
    debugLazy("render called with " + taskNames)
    taskNames match {
      case Full(names) => {
        "#count *" #> names.size &
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


  override def lowPriority = {
    case TaskAdded(name, _) => {
      debugLazy("Task %s added ".format(name))
      Controller.clusterHub ak_! GetTaskNames
    }
    case TaskNames(names) => {
      debugLazy("Names %s received ".format(names.mkString(",")))
      taskNames = Full(names)
      reRender()
    }
  }

  override protected def localSetup() {
    debugLazy("localSetup Called on " + name)
    Controller.clusterHub ak_! RegisterTaskListener(bridge)
    Controller.clusterHub ak_! GetTaskNames
    //    Controller.clusterHub ak_!! GetTaskNames
    //for (a <- answer) mediumPriority(a)
  }

  override protected def localShutdown() {
    logger.debug("localShutdown Called on " + name)
    Controller.clusterHub ak_! DeregisterTaskListener(bridge)
    bridge.stop()
  }
}