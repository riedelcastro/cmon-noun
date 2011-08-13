package org.riedelcastro.cmonnoun.snippet

import net.liftweb.util._
import Helpers._
import org.riedelcastro.cmonnoun.comet.Controller
import org.riedelcastro.cmonnoun.clusterhub.TaskManager.SetTask
import org.riedelcastro.cmonnoun.clusterhub.Mailbox

/**
 * @author sriedel
 */
class Test(param: String) {

  def render = {
    "#comet [name]" #> param
  }
}

class TaskSnippet(val taskName: String)
  extends CometInitializer("task", taskName, SetTask(taskName, Controller.clusterHub))

abstract class CometInitializer[Param](cometId: String, cometName: String, msg: Any) {

  Controller.mailbox ! Mailbox.LeaveMessage(cometName, msg)

  def render = {
    "#%s [name]".format(cometId) #> cometName
  }

}