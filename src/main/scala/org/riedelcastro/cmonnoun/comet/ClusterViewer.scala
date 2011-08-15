package org.riedelcastro.cmonnoun.comet

import net.liftweb.common._
import xml.Text
import akka.actor.ActorRef
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub.{GetClusterManager, AssignedClusterManager}
import net.liftweb.http.js.JE.JsRaw
import org.riedelcastro.cmonnoun.clusterhub.ClusterManager._
import org.riedelcastro.cmonnoun.clusterhub._
import org.riedelcastro.nurupo.HasLogger
import net.liftweb.http.js.JsCmds.{_Noop, SetHtml}
import net.liftweb.http.{FileParamHolder, SHtml, CometActor}
import io.Source

class ClusterViewer extends CallMailboxFirst with HasLogger {

  def cometType = "cluster"

  case class Assignment(clusterId: String, manager: ActorRef)

  private var assignment: Box[Assignment] = Empty

  private var selection: Seq[Row] = Seq.empty

  private var specs: Seq[FieldSpec] = Seq.empty

  private var model: Box[ModelSummary] = Empty

  private var query: Box[Any] = Empty


  override def lowPriority = {
    case SetCluster(name, hub) =>
      assignment = Empty
      selection = Seq.empty
      specs = Seq.empty
      hub ak_! GetClusterManager(name)

    case AssignedClusterManager(manager, clusterId) =>
      assignment = Full(Assignment(clusterId, manager))
      manager ak_! ClusterManager.GetAllRows
      manager ak_! ClusterManager.GetModelSummary
      manager ak_! RegisterListener(bridge)
      reRender()

    case Rows(s, r) =>
      specs = s
      selection = r.toSeq
      reRender()

    case RowsChanged =>
      for (s <- assignment) {
        s.manager ak_! ClusterManager.GetAllRows
      }

    case ModelChanged =>
      for (s <- assignment) {
        s.manager ak_! ClusterManager.GetModelSummary
      }
    case m: ModelSummary =>
      model = Full(m)
      reRender()

  }

  def addContentBinding(a: Assignment) = {
    var content: String = "content"

    Seq(
      "#new_content" #> SHtml.text(content, content = _),
      "#new_content_submit" #>
        SHtml.submit("Add Row", () => a.manager ! AddRow(content))
    ).reduce(_ & _)

  }

  def addFieldSpecBinding(a: Assignment) = {
    var spec: String = "ed$"
    var name: String = "Field" + (specs.size + 1)
    Seq(
      "#new_spec_name" #> SHtml.text(name, name = _),
      "#new_spec" #> SHtml.text(spec, spec = _),
      "#new_spec_submit" #> SHtml.submit("Add Spec",
        () => a.manager ! AddFieldSpec(RegExFieldSpec(name, spec)))
    ).reduce(_ & _)
  }

  def fileUploadBinding() = {

    var fileHolder: Box[FileParamHolder] = Empty

    def upload() {
      for (paramHolder <- fileHolder;
           a <- assignment) {
        val bytes = paramHolder.file
        val lines = Source.fromBytes(bytes).getLines()
        a.manager ak_! ClusterManager.AddRowBatch(lines)
      }
    }

    Seq(
      "#file_upload" #> SHtml.fileUpload(fh=>fileHolder = Full(fh)),
      "#file_upload_submit" #> SHtml.submit("Upload rows",
        () => upload())
    ).reduce(_ & _)

  }

  def render = {
    assignment match {
      case Full(a) =>
        Seq(
          addFieldSpecBinding(a),
          addContentBinding(a),
          fileUploadBinding(),

          ".clusterName"
            #> a.clusterId,

          "#trigger_estep"
            #> SHtml.ajaxButton("E-Step", () => {a.manager ak_! DoEStep; _Noop}),

          "#trigger_mstep"
            #> SHtml.ajaxButton("M-Step", () => {a.manager ak_! DoMStep; _Noop}),

          "#row_field_name *"
            #> specs.map(s => s.name),

          "#sigma_true *"
            #> specs.map(s => model.map(m => m.sigmaTrue.getOrElse(s, 0.5).toString).getOrElse("N/A")),

          "#prior *"
            #> model.map(m => m.prior.toString).getOrElse("N/A"),

          "#row_body *"
            #> selection.map(r => Seq(
            ".content *" #> r.instance.content,
            ".field *" #> specs.map(s => r.instance.fields(s.name).toString),
            ".prob *" #> r.label.prob.toString,
            ".edit *" #> SHtml.ajaxText(r.label.edit.toString, t => {
              a.manager ak_! Edit(r.id, t.toDouble);
              logger.debug("Set to " + t)
            })
            //            ".edit *" #> r.label.edit.toString,
          ).reduce(_ & _))

        ).reduce(_ & _)
      case _ =>
        "#cluster" #> "Nothing here"
    }
  }
}

