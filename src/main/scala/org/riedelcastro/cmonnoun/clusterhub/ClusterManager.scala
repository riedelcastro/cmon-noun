package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{Actor, ActorRef}
import org.riedelcastro.nurupo.HasLogger
import collection.mutable.ArrayBuffer

/**
 * @author sriedel
 */
object ClusterManager {
  case class SetCluster(id: String)
  case object GetAllRows
  case class Rows(specs:Seq[FieldSpec], rows: Iterable[Row])
  case object Train
  case class AddRow(row: Row)
  case class AddFieldSpec(name: String, spec: FieldSpec)
  case object ClusterChanged
}

case class Row(content: String, fields: Map[String, Any], prob: Double)

trait ProbabilisticModel {
  def prob(row:Row):Double = 1.0
}

trait ClusterPersistence extends MongoSupport {

  def addRow(row: Row) {

  }

  def loadRows(): Iterable[Row] = {
    null
  }

  def addFieldToAll(func: String => Any) {

  }


}

class ClusterManager
  extends Actor with HasLogger with ClusterPersistence with HasListeners {

  import ClusterManager._

  case class State(clusterId: String)

  private val specs = new ArrayBuffer[FieldSpec]

  private var state: Option[State] = None

  protected def receive = {

    receiveListeners.orElse {

      case SetCluster(id) =>
        state = Some(State(id))

      case GetAllRows =>
        val rows = loadRows()
        self.reply(Rows(specs,rows))

      case AddRow(row) =>
        addRow(row)
        informListeners(ClusterChanged)

      case AddFieldSpec(name, spec) =>
        addFieldToAll(spec.extract(_))
        informListeners(ClusterChanged)

    }

  }
}