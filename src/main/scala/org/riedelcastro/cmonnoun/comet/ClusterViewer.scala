package org.riedelcastro.cmonnoun.comet

import net.liftweb.common._
import org.riedelcastro.cmonnoun.clusterhub.ClusterManager.SetCluster
import xml.Text
import net.liftweb.http.js.JsCmds.SetHtml
import net.liftweb.http.{SHtml, CometActor}

/**
 * @author sriedel
 */
class ClusterViewer extends CallMailboxFirst {

  def cometType = "cluster"
  case class ViewState(clusterId:String)

  private var state:Box[ViewState] = Empty

  override def lowPriority = {
    case SetCluster(clusterId) =>
      state = Full(ViewState(clusterId))
  }

  def render = {
    state match {
      case Full(s)=>
        "#addRow" #> SHtml.ajaxButton(Text("Add"),() => {
          SetHtml("blah",Text("pubp"))
        }) &
        ".clusterName" #> s.clusterId
      case _ =>
        "#cluster" #> "Nothing here"
    }

  }
}