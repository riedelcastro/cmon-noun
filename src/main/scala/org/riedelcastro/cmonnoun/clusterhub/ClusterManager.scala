package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{Actor, ActorRef}
import org.riedelcastro.nurupo.HasLogger
import org.bson.types.ObjectId
import collection.mutable.HashMap


/**
 * @author sriedel
 */
object ClusterManager {
  case class SetCluster(id: String, hub: ActorRef)
  case object GetAllRows
  case object GetModelSummary
  case class Rows(specs: Seq[FieldSpec], rows: TraversableOnce[Row])
  case object Train
  case class AddRow(content: String)
  case class AddRowBatch(rows:TraversableOnce[String])
  case class AddFieldSpec(spec: FieldSpec)
  case object RowsChanged
  case object ModelChanged
  case class Edit(id: ObjectId, value: Double)

  case object DoEStep
  case object DoMStep

  case object GetDictNames
  case object DictsChanged
  case class DictNames(names:Seq[String])
  case class StoreDictionary(name:String, entries:TraversableOnce[DictEntry])

  case class DictEntry(key:String, score:Double = 1.0)
  case class Dict(name:String, map:Map[String,Double])

}

trait FieldExtractor {
  def spec:FieldSpec
  def extract(content:String):Any
}

class RegexExtractor(val spec:RegExFieldSpec) extends FieldExtractor {
  def extract(content: String) = {
    spec.r.findFirstIn(content).isDefined
  }
}

class DictExtractor(val spec:DictFieldSpec, val dict:Map[String,Double]) extends FieldExtractor {
  def extract(content: String) = {
    val score = dict.getOrElse(content,0.0)
    score >= 1.0
  }
}

class DictScoreExtractor(val spec:DictFieldSpec, val dict:Map[String,Double]) extends FieldExtractor {
  def extract(content: String) = {
    val score = dict.getOrElse(content,0.0)
    score
  }
}



case class RowInstance(content: String,
                       fields: Map[String, Any] = Map.empty)


case class Row(instance: RowInstance,
               label: RowLabel = RowLabel(),
               id: ObjectId = new ObjectId())

case class RowLabel(prob: Double = 0.5,
                    edit: Double = 0.5,
                    penalty: Double = 0.0) {
  def target:Double = {
    if (edit != 0.5) edit else prob
  }
}

case class ModelSummary(prior:Double,
                        sigmaTrue:Map[FieldSpec,Double],
                        sigmaFalse:Map[FieldSpec,Double],
                        gaussTrue:Map[FieldSpec,Double] = Map.empty,
                        gaussFalse:Map[FieldSpec,Double] = Map.empty)

//todo: make this ModelSummary(params:Map[FieldSpec,Param]) and have case class Gaussian2D, and case class Binomial2D



class ClusterManager
  extends Actor with HasLogger with ClusterPersistence with HasListeners with ProbabilisticModel {

  import ClusterManager._

  case class State(clusterId: String)

  protected var state: Option[State] = None

  def createRowFromContent(content: String): Row = {
    val fields = extractors.map(s => s.spec.name -> s.extract(content)).toMap
    val instance = RowInstance(content, fields)
    val row = Row(instance)
    row
  }
  protected def receive = {

    receiveListeners.orElse {

      case SetCluster(id, _) =>
        state = Some(State(id))
        loadDicts()
        loadSpecs()

      case GetAllRows =>
        val rows = loadRows()
        self.reply(Rows(extractors.map(_.spec), rows))

      case AddRow(content) =>
        val row: Row = createRowFromContent(content)
        addRow(row)
        informListeners(RowsChanged)

      case AddRowBatch(rows) =>
        for (r <- rows) {
          addRow(createRowFromContent(r))
        }
        informListeners(RowsChanged)

      case AddFieldSpec(spec) =>
        addSpec(spec)
        informListeners(RowsChanged)

      case Edit(id, value) =>
        editLabel(id, value)

      case DoEStep =>
        eStep()
        informListeners(RowsChanged)

      case DoMStep =>
        mStep()
        informListeners(ModelChanged)

      case GetModelSummary =>
        val summary = ModelSummary(prior, sigmaTrue.toMap, sigmaFalse.toMap)
        self.reply(summary)

      case StoreDictionary(name,entries) =>
        storeDict(name,entries)
        informListeners(DictsChanged)

      case GetDictNames =>
        self.reply(DictNames(dicts.map(_.name)))

    }

  }
}