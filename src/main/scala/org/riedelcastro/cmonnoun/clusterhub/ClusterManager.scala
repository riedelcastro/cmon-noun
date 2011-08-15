package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{Actor, ActorRef}
import org.riedelcastro.nurupo.HasLogger
import org.bson.types.ObjectId
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.Imports._
import collection.mutable.{HashMap, ArrayBuffer}


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
  case class AddFieldSpec(spec: FieldSpec)
  case object RowsChanged
  case object ModelChanged
  case class Edit(id: ObjectId, value: Double)

  case object DoEStep
  case object DoMStep

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
      for (spec <- specs) {
        if (true == row.instance.fields(spec.name)) {
          countsTrue(spec) = countsTrue(spec) + probUse
          countsFalse(spec) = countsFalse(spec) + 1.0 - probUse
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

trait ClusterPersistence extends MongoSupport {

  this: ClusterManager =>

  protected val specs = new ArrayBuffer[FieldSpec]

  private val reserved = Set("_id", "content", "prob", "edit")

  def toMongoFieldName(name: String): String = "_" + name
  def fromMongoFieldName(fieldName: String): String = fieldName.drop(1)

  def addRow(row: Row) {
    for (s <- state) {
      val coll = collFor(s.clusterId, "rows")
      val basic = List(
        "_id" -> row.id,
        "content" -> row.instance.content
      )
      val fields = row.instance.fields.toList.map({case (k, v) => toMongoFieldName(k) -> v})
      val dbo = MongoDBObject(basic ++ fields)
      if (row.label.edit != 0.5) {
        dbo.put("edit", row.label.edit)
      }
      if (row.label.prob != 0.5) {
        dbo.put("prob", row.label.prob)
      }
      coll += dbo
    }
  }

  def loadRows(): TraversableOnce[Row] = {
    val opt = for (s <- state) yield {
      val coll = collFor(s.clusterId, "rows")
      for (dbo <- coll.find()) yield {
        val id = dbo._id.get
        val content = dbo.as[String]("content")
        val prob = dbo.getAs[Double]("prob").getOrElse(0.5)
        val edit = dbo.getAs[Double]("edit").getOrElse(0.5)
        val fields = dbo.filterKeys(!reserved(_))
        val renamed = fields.map({case (k, v) => fromMongoFieldName(k) -> v})
        val label = RowLabel(prob, edit)
        val instance = RowInstance(content, renamed.toMap)
        Row(instance, label, id)
      }
    }
    opt.getOrElse(Seq.empty)
  }

  def evaluateSpecOnRows(spec: FieldSpec) {
    for (s <- state) {
      val coll = collFor(s.clusterId, "rows")
      for (row <- loadRows()) {
        val q = MongoDBObject("_id" -> row.id)
        val set = MongoDBObject("$set" -> MongoDBObject(
          toMongoFieldName(spec.name) -> spec.extract(row.instance.content)
        ))
        coll.update(q, set)
      }

    }
  }

  def addSpec(spec: FieldSpec) {
    for (s <- state) {
      specs += spec
      val coll = collFor(s.clusterId, "specs")
      spec match {
        case RegExFieldSpec(name, regex) =>
          coll += MongoDBObject("type" -> "regex", "name" -> name, "regex" -> regex)
      }
      evaluateSpecOnRows(spec)
    }
  }

  def editLabel(id: ObjectId, value: Double) {
    setRowField(id, "edit", value)
  }

  def setProb(id: ObjectId, value: Double) {
    setRowField(id, "prob", value)
  }

  def setRowField(id: ObjectId, name: String, value: Any) {
    for (s <- state) {
      val coll = collFor(s.clusterId, "rows")
      val q = MongoDBObject("_id" -> id)
      val set = MongoDBObject("$set" -> MongoDBObject(name -> value))
      coll.update(q, set)
    }

  }


  def loadSpecs(): Iterable[FieldSpec] = {
    null
  }


}

class ClusterManager
  extends Actor with HasLogger with ClusterPersistence with HasListeners with ProbabilisticModel {

  import ClusterManager._

  case class State(clusterId: String)

  protected var state: Option[State] = None

  protected def receive = {

    receiveListeners.orElse {

      case SetCluster(id, _) =>
        state = Some(State(id))

      case GetAllRows =>
        val rows = loadRows()
        self.reply(Rows(specs, rows))

      case AddRow(content) =>
        val fields = specs.map(s => s.name -> s.extract(content)).toMap
        val instance = RowInstance(content, fields)
        val row = Row(instance)
        addRow(row)
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

    }

  }
}