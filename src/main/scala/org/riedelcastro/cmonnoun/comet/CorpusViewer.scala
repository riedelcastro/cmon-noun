package org.riedelcastro.cmonnoun.comet

import org.riedelcastro.nurupo.HasLogger
import akka.actor.ActorRef
import net.liftweb.http.SHtml
import org.riedelcastro.cmonnoun.clusterhub.CorpusManager._
import net.liftweb.util.CssSel
import net.liftweb.http.js.JsCmds.{Replace, SetHtml, _Noop}
import xml.{Elem, Text}
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub.{AssignedClusterManager, GetCorpusManager, AssignedCorpusManager}
import org.riedelcastro.cmonnoun.clusterhub.HasListeners.RegisterListener
import org.riedelcastro.cmonnoun.clusterhub.{Row, ClusterManager, ClusterHub}
import collection.mutable.{MultiMap, HashMap}
import org.riedelcastro.cmonnoun.clusterhub.ClusterManager.{GetRowsForSentences, Rows}
import net.liftweb.http.js.JsCmd

/**
 * @author sriedel
 */
class CorpusViewer extends CallMailboxFirst with HasLogger {
  def cometType = "corpus"

  var corpusManager: Option[ActorRef] = None
  var corpusId: Option[String] = None
  var query: Option[SentenceQuery] = None
  var sentences: Option[Seq[Sentence]] = None
  var clusters: Option[Seq[ActorRef]] = None
  case class TokenSelection(spec: TokenSpec, token: Token, localSentenceIndex: Int)
  var tokenSelection: Option[TokenSelection] = None
  val token2rows = new HashMap[TokenSpec, scala.collection.mutable.Set[Row]] with MultiMap[TokenSpec, Row]

  var added = 0

  def toId(tokenSpec: TokenSpec) = {
    "%s_%d_%d".format(tokenSpec.sentence.docId, tokenSpec.sentence.sentenceIndex, tokenSpec.tokenIndex)
  }


  def tokenLink(localSentence: Int, token: Token, spec: TokenSpec, selected: Boolean, default: Elem = null): Elem = {
    val id = toId(spec)
    val clazz = if (selected) "selected" else "normal"
    var result: Elem = null
    result = SHtml.a(() => {
      if (!selected) {
        val undo = tokenSelection match {
          case Some(TokenSelection(s, t, i)) => Replace(toId(s), tokenLink(i, t, s, false, result))
          case None => _Noop
        }
        tokenSelection = Some(TokenSelection(spec, token, localSentence))
        undo & Replace(id, tokenLink(localSentence, token, spec, true, result))
      } else {
        tokenSelection = None
        Replace(id, default)
      }
    }, Text(token.word), SHtml.BasicElemAttr("id", id), SHtml.BasicElemAttr("class", clazz))
    result
  }


  case class TokenState(selected: Boolean, labels: Seq[String]) {
    def asClass = if (selected) "selected" else "normal"
  }

  def deselectCmd() = {
    tokenSelection match {
      case Some(sel) => tokenSwitches(sel.spec).deselect()
      case None => _Noop
    }
  }

  class TokenElemSwitch(val spec: TokenSpec, val token: Token, val localSentenceIndex: Int) extends HashMap[TokenState, Elem] {

    import SHtml._

    lazy val id = toId(spec)

    var state: TokenState = TokenState(false, Seq.empty)

    def setState(newState: TokenState): JsCmd = {
      state = newState
      if (state.selected) {
        tokenSelection = Some(TokenSelection(spec, token, localSentenceIndex))
      }
      Replace(id, elem(state))
    }

    def toggleSelection() = setState(state.copy(selected = !state.selected))

    def deselect() = setState(state.copy(selected = false))


    def elem(state: TokenState = this.state): Elem = {
      getOrElseUpdate(state, {
        state match {
          case s@TokenState(true, _) =>
            SHtml.a(() => {
              toggleSelection()
//              Replace(id, elem(s.copy(selected = false)))
            }, Text(token.word), BasicElemAttr("class", s.asClass), BasicElemAttr("id", id))
          case s@TokenState(false, _) =>
            SHtml.a(() => {
              deselectCmd() & toggleSelection() //Replace(id, elem(s.copy(selected = true)))
            }, Text(token.word), BasicElemAttr("class", s.asClass), BasicElemAttr("id", id))
        }
      })
    }

  }

  val tokenSwitches = new HashMap[TokenSpec, TokenElemSwitch]


  def perSentenceBinding(sents: scala.Seq[Sentence]): Seq[CssSel] = {
    sents.zipWithIndex.map({
      case (s, index) => Seq(
        ".sentence_id *" #> s.docId,
        ".sentence_token *" #> s.tokens.map(t => {
          val spec = TokenSpec(SentenceSpec(s.docId, s.indexInDoc), t.index)
          val switch = tokenSwitches.getOrElseUpdate(spec, new TokenElemSwitch(spec, t, index))
          switch.elem(TokenState(false, Seq.empty))
        })
        //      ".sentence_tokens *" #> s.tokens.map(_.word).mkString(" ")
      ).reduce(_ & _)
    })
  }

  def render = {
    var sentenceToAdd = "Enter sentence here"
    val corpusName = ".corpus_name" #> corpusId.getOrElse("No Name")
    var clusterName: String = ""
    def labelToken() {
      //need to get cluster manager
      for (TokenSelection(spec, token, localSentence) <- tokenSelection;
           sents <- sentences) {
        val manager = Controller.clusterHub ak_!! ClusterHub.GetOrCreateClusterManager(clusterName)
        for (AssignedClusterManager(m, c) <- manager) {
          val sentence = sents(localSentence)
          m ak_!! ClusterManager.AddToken(spec, sentence)
        }
      }
    }
    val addLabelText = "#add_label_text" #> SHtml.text(clusterName, clusterName = _)
    val addLabelSubmit = "#add_label_submit" #> SHtml.submit("Add Cluster", () => labelToken())
    val sentenceAdd = corpusManager match {
      case None => "#sentence_field" #> ""
      case Some(m) => Seq(
        "#sentence_field" #> SHtml.ajaxText(sentenceToAdd, s => {sentenceToAdd = s; _Noop}),
        "#sentence_submit" #> SHtml.ajaxButton("Add Sentence", () => {
          val tokenized = sentenceToAdd.split(" ")
          val tokens = tokenized.zipWithIndex.map({case (w, i) => Token(i, w)})
          val sentence = Sentence("web", added, tokens)
          added += 1
          m ak_! StoreSentence(sentence)
          _Noop
        })
      ).reduce(_ & _)
    }
    val sentencesBinding = sentences match {
      case None => "#sentences" #> "No sentences available"
      case Some(sents) => ".sentence" #> perSentenceBinding(sents)
    }
    Seq(addLabelText, addLabelSubmit, corpusName, sentenceAdd, sentencesBinding).reduce(_ & _)

  }

  def orderAnnotationsForSentences() {
    for (sents <- sentences) {
      //call cluster managers or hub to get all annotations for the given sentences
    }
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
      val sents = s.toSeq
      sentences = Some(sents)
      Controller.clusterHub ak_! GetRowsForSentences(sents.map(_.sentenceSpec))
      reRender()

    case SentenceAdded(s) =>
      for (m <- corpusManager)
        m ak_! SentenceQuery("", 0, 10)

    case Rows(specs, rows, clusterId) =>
      for (row <- rows) {
        token2rows.addBinding(row.spec, row)
      }
    //create

  }
}