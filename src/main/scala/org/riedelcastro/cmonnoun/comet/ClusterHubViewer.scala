package org.riedelcastro.cmonnoun.comet

import net.liftweb.http._
import net.liftweb.common.{Empty, Full, Box}
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub._
import org.riedelcastro.nurupo.HasLogger
import org.riedelcastro.cmonnoun.clusterhub.RegisterListener
import org.riedelcastro.cmonnoun.clusterhub.TaskManager.AddCluster


/**
 * @author sriedel
 */
class ClusterHubViewer extends CometActor with WithBridge with HasLogger {

  private var clusterNames: Seq[String] = Seq.empty
  private val hub = Controller.clusterHub


  def render = {
    debugLazy("render called with " + clusterNames)
    Seq(
    ".cluster *" #> clusterNames.map(name => {
      Seq(
        ".link *" #> name,
        ".link [href]" #> "cluster/%s".format(name)
      ).reduce(_ & _)
    }), {
      var clusterName: String = "Cluster%d".format(clusterNames.size + 1)
      Seq(
        "#new_cluster" #> SHtml.text(
          clusterName, clusterName = _),
        "#new_cluster_submit" #> SHtml.submit(
          "Add", () => hub ! CreateCluster(clusterName))
      ).reduce(_ & _)
    }).reduce(_ & _)


  }


  override def lowPriority = {
    case ClusterAdded(name, _) => {
      debugLazy("Cluster %s added ".format(name))
      hub ak_! GetClusterNames
    }
    case ClusterNames(names) => {
      debugLazy("Names %s received ".format(names.mkString(",")))
      clusterNames = names
      reRender()
    }
  }

  override protected def localSetup() {
    debugLazy("localSetup Called on " + name)
    hub ak_! RegisterListener(bridge)
    hub ak_! GetClusterNames
    //    Controller.clusterHub ak_!! GetTaskNames
    //for (a <- answer) mediumPriority(a)
  }

  override protected def localShutdown() {
    logger.debug("localShutdown Called on " + name)
    Controller.clusterHub ak_! DeregisterTaskListener(bridge)
    bridge.stop()
  }
}