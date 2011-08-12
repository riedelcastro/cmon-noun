package org.riedelcastro.cmonnoun.snippet

import net.liftweb.util._
import Helpers._
import org.riedelcastro.cmonnoun.comet.Controller
import scala.Some
import xml.Text
import net.liftweb.http.SHtml
import org.riedelcastro.cmonnoun.clusterhub.{AddInstance, Instances, GetInstances}
import org.riedelcastro.nurupo.HasLogger

/**
 * @author sriedel
 */
class ProblemPage(problemName: String) {

  var problem: Option[Instances] = Controller.problemManager !! GetInstances(problemName) match {
    case Some(Some(Instances(instances))) => Some(Instances(instances))
    case _ => None
  }

  def render = {
    problem match {
      case Some(Instances(instances)) =>
        "#problemName *" #> problemName &
          ".instance *" #> instances.map(_.content)
      case None =>
        "#problem" #> Text("Can't find problem with name " + problemName)
    }
  }
}

class InstanceAdder(problemName: String) extends HasLogger {
  def render = {
    var word: String = "whatever"
    def add() {
      logger.info("Send %s to problemManager".format(word))
      Controller.problemManager ! AddInstance(problemName, word)
    }
    "name=word" #> SHtml.onSubmit({w => logger.info("Set " + w); word = w; add()})
//    & "type=submit" #> SHtml.onSubmitUnit(add)
  }
}