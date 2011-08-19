package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{Actor, ActorRef}
import org.riedelcastro.nurupo.HasLogger
import org.bson.types.ObjectId
import collection.mutable.HashMap
import org.riedelcastro.cmonnoun.clusterhub.CorpusManager.{SentenceSpec, TokenSpec, InstanceSpec}

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
  case class AddRowBatch(rows: TraversableOnce[String])
  case class AddFieldSpec(spec: FieldSpec)
  case object RowsChanged
  case object ModelChanged
  case class Edit(id: ObjectId, value: Double)

  case object DoEStep
  case object DoMStep
  case class DoEM(iterations: Int)

  case object GetDictNames
  case object DictsChanged
  case class DictNames(names: Seq[String])
  case class StoreDictionary(name: String, entries: TraversableOnce[DictEntry])

  case class DictEntry(key: String, score: Double = 1.0)
  case class Dict(name: String, map: Map[String, Double])

  case object ResetModel

  case class DoQuery(query: Query)
  case object DoRandomQuery

  sealed trait Sorting
  case object SortByProb extends Sorting
  case object SortByContent extends Sorting
  case class Query(content: String, sorting: Sorting, from: Int, batchSize: Int, ascending: Boolean = false)

  /**
   * Get the rows that label the given instances.
   */
  case class GetRowsForSentences(instances: Seq[SentenceSpec])

}


trait FieldExtractor {
  def spec: FieldSpec
  def extract(content: String): Any
}

class RegexExtractor(val spec: RegExFieldSpec) extends FieldExtractor {
  def extract(content: String) = {
    spec.r.findFirstIn(content).isDefined
  }
}

class DictExtractor(val spec: DictFieldSpec, val dict: Map[String, Double]) extends FieldExtractor {
  def extract(content: String) = {
    val score = dict.getOrElse(content, 0.0)
    score >= 1.0
  }
}

class DictScoreExtractor(val spec: DictFieldSpec, val dict: Map[String, Double]) extends FieldExtractor {
  def extract(content: String) = {
    val score = dict.getOrElse(content, 0.0)
    score
  }
}


case class RowInstance(content: String,
                       fields: Map[String, Any] = Map.empty)


case class Row(instance: RowInstance,
               label: RowLabel = RowLabel(),
               id: ObjectId = new ObjectId(),
               spec: TokenSpec = TokenSpec(SentenceSpec("blah", 0), 0))

case class RowLabel(prob: Double = 0.5,
                    edit: Double = 0.5,
                    penalty: Double = 0.0) {
  def target: Double = {
    if (edit != 0.5) edit else prob
  }
}

case class ModelSummary(prior: Double,
                        sigmaTrue: Map[FieldSpec, Double],
                        sigmaFalse: Map[FieldSpec, Double],
                        gaussTrue: Map[FieldSpec, (Double, Double)] = Map.empty,
                        gaussFalse: Map[FieldSpec, (Double, Double)] = Map.empty) {

  def forTrue(spec: FieldSpec) = {
    sigmaTrue.get(spec).map("%1.3f".format(_)).getOrElse(gaussTrue.get(spec).map(p => "%1.3f %1.3f".format(p._1, p._2)).getOrElse("N/A"))
  }

  def forFalse(spec: FieldSpec) = {
    sigmaFalse.get(spec).map("%1.3f".format(_)).getOrElse(gaussFalse.get(spec).map(p => "%1.3f %1.3f".format(p._1, p._2)).getOrElse("N/A"))
  }


}

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

      case DoEM(iterations) =>
        for (i <- 0 until iterations) {
          mStep()
          eStep()
        }
        informListeners(RowsChanged)
        informListeners(ModelChanged)


      case GetModelSummary =>
        val summary = ModelSummary(prior,
          distributionsTrue.binomialMap(), distributionsFalse.binomialMap(),
          distributionsTrue.gaussianMap(), distributionsFalse.gaussianMap())
        self.reply(summary)

      case StoreDictionary(name, entries) =>
        storeDict(name, entries)
        informListeners(DictsChanged)

      case GetDictNames =>
        self.reply(DictNames(dicts.map(_.name)))

      case ResetModel =>
        resetModel()
        informListeners(ModelChanged)

      case DoQuery(query) =>
        val rows = this.query(query)
        self.reply(Rows(extractors.map(_.spec), rows))

      case DoRandomQuery =>
        val rows = randomRows()
        self.reply(Rows(extractors.map(_.spec), rows))

      case GetRowsForSentences(sentences) =>
        val result = for (s <- sentences;
                          rows <- loadRowsForSentence(s.docId, s.sentenceIndex).toSeq;
                          row <- rows) yield row
        self.reply(Rows(extractors.map(_.spec), result))
    }

  }
}