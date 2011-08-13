package org.riedelcastro.cmonnoun.comet

import net.liftweb.http.{SHtml, CometActor}
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub.CreateTask

/**
 * @author sriedel
 */
class TaskCreator extends CometActor with WithBridge {
  def render = {
    SHtml.ajaxForm(
      <span>What's your name?</span> ++
        /* SHtml.text generates a text input that invokes a Scala
         * callback (in this case, the login method) with the text
         * it contains when the form is submitted. */
        SHtml.text("", addProblem) ++
          <input type="submit" value="Create Problem"/>
    )
  }

  def addProblem(name:String) {
    Controller.clusterHub ! CreateTask(name)
  }
}