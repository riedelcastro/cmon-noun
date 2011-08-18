package org.riedelcastro.cmonnoun.comet

import org.riedelcastro.nurupo.HasLogger
import akka.actor.ActorRef
import org.riedelcastro.cmonnoun.clusterhub.CorpusManager.{SetCorpus, SentenceQuery}
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub.{GetCorpusManager, AssignedCorpusManager}

/**
 * @author sriedel
 */
class CorpusViewer extends CallMailboxFirst with HasLogger {
  def cometType = "corpus"

  var corpusManager:Option[ActorRef] = None
  var corpusId:Option[String] = None
  var query:Option[SentenceQuery] = None

  def render = {
    ".corpus_name" #> corpusId.getOrElse("No Name")
  }

  override def lowPriority = {

    case SetCorpus(id) =>
      corpusId = Some(id)
      Controller.clusterHub ak_! GetCorpusManager(id)

    case AssignedCorpusManager(manager,id) =>
      corpusManager = Some(manager)
      corpusId = Some(id)

  }
}