package org.riedelcastro.cmonnoun.comet

import akka.actor.ActorRef
import org.riedelcastro.cmonnoun.comet.ProgressMonitorComet.SetProgressMonitor
import org.riedelcastro.cmonnoun.clusterhub.{HasListeners, ProgressMonitor}
import net.liftweb.http.{SHtml, CometActor}
import xml.Text

/**
 * @author sriedel
 */
class ProgressMonitorComet extends WithBridge with CometActor {

  var lastProgress:Option[ProgressMonitor.Progress] = None
  var monitor:Option[ActorRef] = None

  override def lowPriority = {
    case SetProgressMonitor(m) => {
      m ! HasListeners.RegisterListener(bridge)
      monitor = Some(m)
    }

    case p:ProgressMonitor.Progress =>
      lastProgress = Some(p)
      reRender(false)
  }


  override protected def localShutdown() {
    super.localShutdown()
    for (m <- monitor) {
      m ! HasListeners.DeregisterListener(bridge)
    }
  }

  def render = {
    "#monitor" #> Text(lastProgress.map(p => "%d / %d".format(p.solved, p.of)).getOrElse("N/A"))
  }
}

object ProgressMonitorComet {
  case class SetProgressMonitor(monitor:ActorRef)
}