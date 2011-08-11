package org.riedelcastro.cmonnoun.comet

import net.liftweb.http.{SessionVar, CometActor}
import net.liftweb.common.{Full, Box}
import org.riedelcastro.cmonnoun.clusterhub.{ProblemNames, DeregisterProblemListConsumer, RegisterProblemListConsumer}
case class ProblemListDisplayState(names:Seq[String])

/**
 * @author sriedel
 */
class ProblemListDisplay extends CometActor with WithBridge {

  object currentState extends SessionVar[Box[ProblemListDisplayState]](Full(ProblemListDisplayState(Seq.empty)))


  def render = {
    currentState.is match {
      case Full(ProblemListDisplayState(names)) => {
        ".problem *" #> names
      }
      case _ =>  {
        ".problem" #> "Empty"
      }
    }
  }


  override def mediumPriority = {
    case ProblemNames(names) => {
      currentState.set(Full(ProblemListDisplayState(names)))
      reRender()
    }
  }

  override protected def localSetup() {
    Controller.problemManager ! RegisterProblemListConsumer(bridge)
  }

  override protected def localShutdown() {
    Controller.problemManager ! DeregisterProblemListConsumer(bridge)
    bridge.stop()
  }
}