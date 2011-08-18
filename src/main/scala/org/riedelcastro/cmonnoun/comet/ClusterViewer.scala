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

  implicit def toStringable(v: Double) = new AnyRef {
    def s = "%1.3f".format(v)
  }

  implicit def toString(v: Boolean) = v.toString()

  def cometType = "cluster"

  case class Assignment(clusterId: String, manager: ActorRef)

  private var assignment: Box[Assignment] = Empty

  private var selection: Seq[Row] = Seq.empty

  private var specs: Seq[FieldSpec] = Seq.empty

  private var dicts: Seq[String] = Seq.empty

  private var model: Box[ModelSummary] = Empty

  private var query: Query = Query("", SortByProb, 0, 10, false)


  override def lowPriority = {
    case SetCluster(name, hub) =>
      assignment = Empty
      selection = Seq.empty
      specs = Seq.empty
      hub ak_! GetClusterManager(name)

    case AssignedClusterManager(manager, clusterId) =>
      assignment = Full(Assignment(clusterId, manager))
      manager ak_! ClusterManager.DoQuery(query)
      manager ak_! ClusterManager.GetModelSummary
      manager ak_! ClusterManager.GetDictNames
      manager ak_! RegisterListener(bridge)
      reRender()

    case Rows(s, r) =>
      specs = s
      selection = r.toSeq
      reRender()

    case RowsChanged =>
      for (s <- assignment) {
        s.manager ak_! ClusterManager.DoQuery(query)
      }

    case ModelChanged =>
      for (s <- assignment) {
        s.manager ak_! ClusterManager.GetModelSummary
      }

    case m: ModelSummary =>
      model = Full(m)
      reRender()

    case DictsChanged =>
      for (s <- assignment) {
        s.manager ak_! ClusterManager.GetDictNames
      }

    case DictNames(names) =>
      dicts = names
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

  def addDictFieldSpecBinding(a: Assignment) = {
    var gaussian = false
    var name: String = "Field" + (specs.size + 1)
    var dict: String = "dict"
    Seq(
      "#new_dict_spec_gaussian" #> SHtml.checkbox(gaussian, gaussian = _),
      "#new_dict_spec_dicts" #> SHtml.select(dicts.map(m => m -> m), dicts.headOption, dict = _),
      "#new_dict_spec_name" #> SHtml.text(name, name = _),
      "#new_dict_spec_submit" #> SHtml.submit("Add Dict Spec",
        () => a.manager ! AddFieldSpec(DictFieldSpec(name, dict, gaussian)))
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
      "#file_upload" #> SHtml.fileUpload(fh => fileHolder = Full(fh)),
      "#file_upload_submit" #> SHtml.submit("Upload rows",
        () => upload())
    ).reduce(_ & _)

  }

  def dictUploadBinding() = {

    var fileHolder: Box[FileParamHolder] = Empty
    var dictName: String = "dict"

    def upload() {
      for (paramHolder <- fileHolder;
           a <- assignment) {
        val bytes = paramHolder.file
        val lines = Source.fromBytes(bytes).getLines()
        def toEntry(line: String) = {
          val split = line.trim().split("\t")
          if (split.length == 2)
            ClusterManager.DictEntry(split(0), split(1).toDouble)
          else
            ClusterManager.DictEntry(split(0), 1.0)
        }
        a.manager ak_! ClusterManager.StoreDictionary(dictName, lines.map(toEntry(_)))
      }
    }

    Seq(
      "#dict_upload_name" #> SHtml.text(dictName, dictName = _),
      "#dict_upload" #> SHtml.fileUpload(fh => fileHolder = Full(fh)),
      "#dict_upload_submit" #> SHtml.submit("Upload Dict",
        () => upload())
    ).reduce(_ & _)

  }


  def render = {
    assignment match {
      case Full(a) =>

        def queryFor(text: String) = ClusterManager.Query(text, SortByProb, 0, 10, false)

        Seq(
          addFieldSpecBinding(a),
          addDictFieldSpecBinding(a),
          addContentBinding(a),
          fileUploadBinding(),
          dictUploadBinding(),

          ".clusterName"
            #> a.clusterId,

          "#trigger_estep"
            #> SHtml.ajaxButton("E-Step", () => {a.manager ak_! DoEStep; _Noop}),

          "#trigger_mstep"
            #> SHtml.ajaxButton("M-Step", () => {a.manager ak_! DoMStep; _Noop}),

          "#do_em"
            #> SHtml.ajaxButton("EM", () => {a.manager ak_! DoEM(5); _Noop}),

          "#reset_model"
            #> SHtml.ajaxButton("Reset", () => {a.manager ak_! ResetModel; _Noop}),

          "#row_search"
            #> SHtml.ajaxText(query.content, t => {
            query = queryFor(t)
            a.manager ak_! DoQuery(query)
          }),

          "#random_search"
            #> SHtml.ajaxButton("Random", () => {a.manager ak_! DoRandomQuery; _Noop}),

          "#prev"
            #> SHtml.a(() => {
            query = query.copy(from = query.from - 10)
            a.manager ak_! DoQuery(query)
            _Noop
          }, Text("prev"), SHtml.BasicElemAttr("disabled", query.from <= 0)),

          "#next"
            #> SHtml.a(() => {
            query = query.copy(from = query.from + 10)
            a.manager ak_! DoQuery(query)
          }, Text("next")),

          "#row_field_name *"
            #> specs.map(s => s.name),

          "#sigma_true *"
            #> specs.map(s => model.map(m => m.forTrue(s)).getOrElse("N/A")),

          "#sigma_false *"
            #> specs.map(s => model.map(m => m.forFalse(s)).getOrElse("N/A")),

          "#prior_true *"
            #> model.map(m => m.prior.s).getOrElse("N/A"),

          "#prior_false *"
            #> model.map(m => (1.0 - m.prior).s).getOrElse("N/A"),

          "#row_body *"
            #> selection.map(r => Seq(
            ".content *" #> r.instance.content,
            ".field *" #> specs.map(s => r.instance.fields(s.name).toString),
            ".prob *" #> r.label.prob.s,
            ".edit *" #> SHtml.ajaxText(r.label.edit.s, t => {
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

