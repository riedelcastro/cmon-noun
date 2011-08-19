package org.riedelcastro.cmonnoun.comet

import org.riedelcastro.nurupo.HasLogger
import akka.actor.ActorRef
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub.{GetCorpusManager, AssignedCorpusManager}
import net.liftweb.http.SHtml
import org.riedelcastro.cmonnoun.clusterhub.CorpusManager._
import net.liftweb.http.js.JsCmds.{SetHtml, _Noop}
import net.liftweb.util.CssSel
import org.riedelcastro.cmonnoun.clusterhub.RegisterListener

/**
 * @author sriedel
 */
class CorpusViewer extends CallMailboxFirst with HasLogger {
  def cometType = "corpus"

  var corpusManager: Option[ActorRef] = None
  var corpusId: Option[String] = None
  var query: Option[SentenceQuery] = None
  var sentences: Option[Seq[Sentence]] = None
  var clusters:Option[Seq[ActorRef]] = None

  def perSentenceBinding(sents: scala.Seq[Sentence]): Seq[CssSel] = {
    sents.map(s => Seq(
      ".sentence_id *" #> s.docId,
      ".sentence_tokens *" #> s.tokens.map(_.word).mkString(" ")
    ).reduce(_ & _)
    )
  }
  def render = {
    var sentenceToAdd = "Enter sentence here"
    val corpusName = ".corpus_name" #> corpusId.getOrElse("No Name")
    val sentenceAdd = corpusManager match {
      case None => "#sentence_field" #> ""
      case Some(m) => Seq(
        "#sentence_field" #> SHtml.ajaxText(sentenceToAdd, s => {sentenceToAdd = s; _Noop}),
        "#sentence_submit" #> SHtml.ajaxButton("Add Sentence", () => {
          val tokenized = sentenceToAdd.split(" ")
          val tokens = tokenized.zipWithIndex.map({case (w, i) => Token(i, w)})
          val sentence = Sentence("web", 0, tokens)
          m ak_! StoreSentence(sentence)
          _Noop
        })
      ).reduce(_ & _)
    }
    val sentencesBinding = sentences match {
      case None => "#sentences" #> "No sentences available"
      case Some(sents) => ".sentence" #> perSentenceBinding(sents)
    }
    Seq(corpusName, sentenceAdd, sentencesBinding).reduce(_ & _)

  }

  override def lowPriority = {

    case SetCorpus(id) =>
      corpusId = Some(id)
      Controller.clusterHub ak_! GetCorpusManager(id)
      reRender()

    case AssignedCorpusManager(manager, id) =>
      corpusManager = Some(manager)
      corpusId = Some(id)
      manager ak_! RegisterListener(bridge)
      manager ak_! SentenceQuery("", 0, 10)
      reRender()

    case Sentences(s) =>
      sentences = Some(s.toSeq)
      reRender()

    case SentenceAdded(s) =>
      for (m <- corpusManager)
        m ak_! SentenceQuery("", 0, 10)


  }
}