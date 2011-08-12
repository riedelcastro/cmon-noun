package org.riedelcastro.cmonnoun.comet

import net.liftweb.common.{Full, Box}
import org.riedelcastro.cmonnoun.clusterhub.{ProblemNames, DeregisterProblemListConsumer, RegisterProblemListConsumer}
import net.liftweb.http.js.JsCmds.RedirectTo
import xml.Text
import net.liftweb.http._

case class ProblemListDisplayState(names: Seq[String])

/**
 * @author sriedel
 */
class ProblemListDisplay extends CometActor with WithBridge {

  object currentState extends SessionVar[Box[ProblemListDisplayState]](Full(ProblemListDisplayState(Seq.empty)))

//  for (sess <- S.session) sess.sendCometActorMessage(
//    "CometClassNameHere", Full("messageAsString"), cometNameIThink)

  def render = {
    currentState.is match {
      case Full(ProblemListDisplayState(names)) => {
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