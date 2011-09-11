package org.riedelcastro.cmonnoun.snippet

import net.liftweb.util._
import Helpers._
import org.riedelcastro.cmonnoun.comet.Controller
import org.riedelcastro.cmonnoun.clusterhub.TaskManager.SetTask
import org.riedelcastro.cmonnoun.clusterhub.Mailbox
import org.riedelcastro.cmonnoun.clusterhub.ClusterManager.SetCluster
import org.riedelcastro.cmonnoun.clusterhub.CorpusService.SetCorpus
import org.riedelcastro.cmonnoun.comet.EntityListViewer.SetParams

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

class ClusterListSnippet(val taskName: String)
  extends CometInitializer("clusterList", taskName, SetTask(taskName, Controller.clusterHub))


case class ClusterParam(taskName: String, clusterId: String)
case object Noop
class ClusterSnippet(val param: String) extends CometInitializer(
  "cluster",
  param,
  SetCluster(param, Controller.clusterHub))

class EntitiesSnippet(val param: String) extends CometInitializer(
  "entities",
  param,
  SetParams(param,Some("freebasenyt")))


class CorpusSnippet(val param: String) extends CometInitializer("corpus", param, SetCorpus(param))


class ClusterSnippet2(val param: ClusterParam) {
  def render = "#blah" #> param.toString
}

object CometInitializer {
  def name(cometType: String, cometName: String) = cometType + "." + cometName
}

abstract class CometInitializer[Param](cometId: String, cometName: String, msg: Any) {

  Controller.mailbox ! Mailbox.LeaveMessage(CometInitializer.name(cometId, cometName), msg)

  def render = {
    "#%s [name]".format(cometId) #> CometInitializer.name(cometId, cometName)
  }

}