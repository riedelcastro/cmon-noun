package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{Actor, ActorRef}
import org.riedelcastro.nurupo.HasLogger
import collection.mutable.ArrayBuffer
import org.bson.types.ObjectId
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.Imports._


/**
 * @author sriedel
 */
object ClusterManager {
  case class SetCluster(id: String, hub: ActorRef)
  case object GetAllRows
  case class Rows(specs: Seq[FieldSpec], rows: TraversableOnce[Row])
  case object Train
  case class AddRow(content: String)
  case class AddFieldSpec(spec: FieldSpec)
  case object ClusterChanged
  case class Edit(id:ObjectId, value:Double)
}

case class RowInstance(content: String,
                       fields: Map[String, Any] = Map.empty)


case class Row(instance: RowInstance,
               label: RowLabel = RowLabel(),
               id: ObjectId = new ObjectId())

case class RowLabel(prob: Double = 0.5,
                    edit: Double = 0.5,
                    penalty: Double = 0.0)

trait ProbabilisticModel {
  def prob(row: Row): Double = 1.0
}

trait ClusterPersistence extends MongoSupport {

  this: ClusterManager =>

  protected val specs = new ArrayBuffer[FieldSpec]

  private val reserved = Set("_id","content","prob","edit")

  def toMongoFieldName(name:String):String = "_" + name
  def fromMongoFieldName(fieldName:String):String = fieldName.drop(1)

  def addRow(row: Row) {
    for (s <- state) {
      val coll = collFor(s.clusterId, "rows")
      val basic = List(
        "_id" -> row.id,
        "content" -> row.instance.content
      )
      val fields = row.instance.fields.toList.map({case (k,v) => toMongoFieldName(k) -> v})
      val dbo = MongoDBObject(basic ++ fields)
      if (row.label.edit != 0.5) {
        dbo.put("edit",row.label.edit)
      }
      if (row.label.prob != 0.5) {
        dbo.put("prob",row.label.prob)
      }
      coll += dbo
    }
  }

  def loadRows(): TraversableOnce[Row] = {
    val opt = for (s <- state) yield {
      val coll = collFor(s.clusterId,"rows")
      for (dbo <- coll.find()) yield {
        val id = dbo._id.get
        val content = dbo.as[String]("content")
        val prob = dbo.getAs[Double]("prob").getOrElse(0.5)
        val edit = dbo.getAs[Double]("edit").getOrElse(0.5)
        val fields = dbo.filterKeys(!reserved(_))
        val renamed = fields.map({case (k,v)=> fromMongoFieldName(k)->v})
        val label = RowLabel(prob,edit)
        val instance = RowInstance(content,renamed.toMap)
        Row(instance,label,id)
      }
    }
    opt.getOrElse(Seq.empty)
  }

  def evaluateSpecOnRows(spec:FieldSpec) {
    for (s <- state) {
      val coll = collFor(s.clusterId,"rows")
      for (row <- loadRows()){
        val q = MongoDBObject("_id" -> row.id)
        val set = MongoDBObject("$set" -> MongoDBObject(
          toMongoFieldName(spec.name) -> spec.extract(row.instance.content)
        ))
        coll.update(q,set)
      }

    }
  }

  def addSpec(spec: FieldSpec) {
    for (s <- state){
      specs += spec
      val coll = collFor(s.clusterId,"specs")
      spec match {
        case RegExFieldSpec(name,regex) =>
          coll += MongoDBObject("type" -> "regex","name" -> name,"regex"->regex)
      }
      evaluateSpecOnRows(spec)
    }
  }

  def editLabel(id:ObjectId, value:Double) {
    for (s <- state) {
      val coll = collFor(s.clusterId,"rows")
      val q = MongoDBObject("_id" -> id)
      val set = MongoDBObject("$set" -> MongoDBObject("edit" -> value))
      coll.update(q,set)
    }
  }

  def loadSpecs(): Iterable[FieldSpec] = {
    null
  }


}

class ClusterManager
  extends Actor with HasLogger with ClusterPersistence with HasListeners {

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
        val instance = RowInstance(content,fields)
        val row = Row(instance)
        addRow(row)
        informListeners(ClusterChanged)

      case AddFieldSpec(spec) =>
        addSpec(spec)
        informListeners(ClusterChanged)

      case Edit(id,value) =>
        editLabel(id,value)
    }

  }
}