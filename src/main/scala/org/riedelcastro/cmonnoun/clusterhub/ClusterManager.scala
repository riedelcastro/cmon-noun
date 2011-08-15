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


case class RowInstance(content: String,
                       fields: Map[String, Any] = Map.empty)


case class Row(instance: RowInstance,
               label: RowLabel = RowLabel(),
               id: ObjectId = new ObjectId())

case class RowLabel(prob: Double = 0.5,
                    edit: Double = 0.5,
                    penalty: Double = 0.0)

case class ModelSummary(prior:Double, sigmaTrue:Map[FieldSpec,Double], sigmaFalse:Map[FieldSpec,Double])

trait ProbabilisticModel {

  this: ClusterManager =>

  var prior = 0.5

  class Sigma(init: Double = 0.5) extends HashMap[FieldSpec, Double]() {
    override def default(key: FieldSpec) = init
  }


  val sigmaTrue = new Sigma(0.5)
  val sigmaFalse = new Sigma(0.5)

  def eStep() {
    for (row <- loadRows()) {
      var likelihoodTrue = prior
      var likelihoodFalse = 1.0 - prior
      for ((spec, p) <- sigmaTrue) {
        val value = true == row.instance.fields(spec.name)
        likelihoodTrue *= (if (value) p else 1.0 - p)
        likelihoodFalse *= (if (value) 1.0 - p else p)
      }
      val normalizer = likelihoodTrue + likelihoodFalse
      val prob = likelihoodTrue / normalizer
      setProb(row.id, prob)
    }
  }

  def mStep() {

    val countsTrue = new Sigma(0.0)
    val countsFalse = new Sigma(0.0)
    var count = 0
    var totalTrue = 0.0
    for (row <- loadRows()) {
      val prob = row.label.prob
      val probUse = if (row.label.edit != 0.5) row.label.edit else prob
      totalTrue += probUse
      for (spec <- extractors) {
        if (true == row.instance.fields(spec.spec.name)) {
          countsTrue(spec.spec) = countsTrue(spec.spec) + probUse
          countsFalse(spec.spec) = countsFalse(spec.spec) + 1.0 - probUse
        }
      }
      count += 1
    }
    prior = totalTrue / count
    for ((k,v) <- countsTrue) {
      sigmaTrue(k) = v / totalTrue
    }
    for ((k,v) <- countsFalse) {
      sigmaFalse(k) = v / (1.0 - totalTrue)
    }


  }

  def prob(row: Row): Double = 1.0
}



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

      case GetDictNames =>
        self.reply(DictNames(dicts.map(_.name)))

    }

  }
}