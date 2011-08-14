package org.riedelcastro.cmonnoun.comet

import net.liftweb.common._
import xml.Text
import net.liftweb.http.js.JsCmds.SetHtml
import net.liftweb.http.{SHtml, CometActor}
import akka.actor.ActorRef
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub.{GetClusterManager, AssignedClusterManager}
import net.liftweb.http.js.JE.JsRaw
import org.riedelcastro.cmonnoun.clusterhub.ClusterManager._
import org.riedelcastro.cmonnoun.clusterhub._
import org.riedelcastro.nurupo.HasLogger

class ClusterViewer extends CallMailboxFirst with HasLogger {

  def cometType = "cluster"

  case class Assignment(clusterId: String, manager: ActorRef)

  private var assignment: Box[Assignment] = Empty

  private var selection: Seq[Row] = Seq.empty

  private var specs: Seq[FieldSpec] = Seq.empty

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
      manager ak_! RegisterListener(bridge)
      reRender()

    case Rows(s, r) =>
      specs = s
      selection = r.toSeq
      reRender()

    case ClusterChanged =>
      for (s <- assignment) {
        s.manager ak_! ClusterManager.GetAllRows
      }
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

  def render = {
    assignment match {
      case Full(a) =>
        Seq(
          addFieldSpecBinding(a),
          addContentBinding(a),

          ".clusterName" #> a.clusterId,

          "#row_field_name *" #> specs.map(s => s.name),
          "#row_body *" #> selection.map(r => Seq(
            ".content *" #> r.instance.content,
            ".prob *" #> r.label.prob.toString,
            //            ".edit *" #> r.label.edit.toString,
            ".edit *" #> SHtml.ajaxText(
              r.label.edit.toString,
              t => {a.manager ak_! Edit(r.id, t.toDouble); logger.debug("Set to " + t)}),
            ".field *" #> specs.map(s => r.instance.fields(s.name).toString)
          ).reduce(_ & _))

        ).reduce(_ & _)
      case _ =>
        "#cluster" #> "Nothing here"
    }
  }
}

//
///**
// * @author sriedel
// */
//class ClusterViewer extends CallMailboxFirst {
//

//
//  def render = {
//    assignment match {
//      case Full(s) =>
//      "#a" #> "b"
//
////      {
////        {
////          var newContentName: String = "x"
////          def setNewContentName(name: String) = {
////            newContentName = name
////            JsRaw("alert(�Button2 clicked�)")
////          }
////
////          "#newContentName" #> SHtml.ajaxText("What?", setNewContentName) &
////            "#addRow" #> SHtml.ajaxButton(Text("Add"), () => {
////              s.manager ! AddRow("Test")
////              SetHtml("blah", Text("pubp"))
////            }) &
////            ".clusterName" #> s.clusterId
////        }.&
////
////        {
////          "#rowBody *" #> selection.map(i => {
////            ".content *" #> i.content &
////              ".field *" #> i.fields.map(_._2.toString)
////          })
////        }
////      }
//
//
//      case _ =>
//        "#cluster" #> "Nothing here"
//    }
//
//  }
//}