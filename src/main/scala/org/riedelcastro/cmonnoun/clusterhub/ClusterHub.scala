package org.riedelcastro.cmonnoun.clusterhub

import com.novus.salat.annotations
import annotations.raw.Salat
import com.mongodb.casbah.Imports._
import net.liftweb.common.{Full, Empty, Box}
import Box._
import collection.mutable.{HashMap, HashSet, ArrayBuffer}
import org.riedelcastro.cmonnoun.clusterhub.Mailbox.{NoSuchMessage, RetrieveMessage, LeaveMessage}
import org.riedelcastro.nurupo.HasLogger
import org.riedelcastro.cmonnoun.clusterhub.TaskManager.SetTask
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub.{DeregisterTaskListener, RegisterTaskListener}

import com.novus.salat._
import com.novus.salat.global._
import org.bson.types.ObjectId
import com.mongodb.casbah.commons.MongoDBObject
import akka.actor.{Actor, Actors, ActorRef}
import org.riedelcastro.cmonnoun.clusterhub.ClusterManager.SetCluster
import org.riedelcastro.cmonnoun.clusterhub.CorpusManager.SetCorpus

class MutableClusterTask(var name: String) {
  val instances = new ArrayBuffer[Instance]
  val fieldSpecs = new ArrayBuffer[FieldSpec]
}

case class Instance(content: String, fields: Map[String, AnyRef], id: ObjectId = new ObjectId()) {
}

@Salat
trait FieldSpec {
  def name: String
  def realValued: Boolean
}

case class SpecHolder(spec: FieldSpec)

case class RegExFieldSpec(name: String, regex: String) extends FieldSpec {
  val r = regex.r
  def realValued = false
}

case class DictFieldSpec(name: String, dictName: String, gaussian: Boolean = false) extends FieldSpec {
  def realValued = gaussian
}


trait MongoSupport {
  def dbName: String = "clusterhub"
  val mongoConn = MongoConnection("localhost", 27017)
  val mongoDB = mongoConn(dbName)
  def collFor(name: String, param: String): MongoCollection = {
    mongoDB(name + "_" + param)
  }
  def collFor(prefix: String, name: String, param: String): MongoCollection = {
    mongoDB(prefix + "_" + name + "_" + param)
  }

}

case class RegisterListener(l: ActorRef)
case class DeregisterListener(l: ActorRef)


trait HasListeners {
  private val taskListeners = new HashSet[ActorRef]

  def informListeners(msg: Any) {
    for (l <- taskListeners) l ! msg

  }

  def addListener(l: ActorRef) {
    taskListeners += l
  }

  def removeListener(l: ActorRef) {
    taskListeners -= l
  }

  def receiveListeners: PartialFunction[Any, Unit] = {
    case RegisterListener(c) =>
      addListener(c)

    case DeregisterListener(c) =>
      removeListener(c)
  }

}

object ClusterHub {
  case class CreateCluster(name: String)
  case class ClusterAdded(clusterId: String, manager: ActorRef)
  case object GetClusterNames
  case class ClusterNames(names: Seq[String])
  case class GetClusterManager(clusterId: String)
  case class GetOrCreateClusterManager(clusterId: String)
  case class AssignedClusterManager(manager: ActorRef, clusterId: String)

  case class CreateTask(name: String)
  case class TaskNames(names: Seq[String])
  case class RegisterTaskListener(consumer: ActorRef)
  case class DeregisterTaskListener(consumer: ActorRef)
  case object GetTaskNames
  case class TaskAdded(taskName: String, manager: ActorRef)
  case class GetTaskManager(taskName: String)
  case class AssignedTaskManager(manager: Box[ActorRef])

  case class GetCorpusManager(corpusId: String)
  case class AssignedCorpusManager(manager: ActorRef, corpusId: String)

}


/**
 * @author sriedel
 */
class ClusterHub extends Actor with MongoSupport with HasListeners with HasLogger {

  import ClusterHub._

  private val taskManagers = new HashMap[String, ActorRef]
  private val clusterManagers = new HashMap[String, ActorRef]
  private val corpusManagers = new HashMap[String, ActorRef]


  private val taskDefColl = mongoDB("tasks")
  private val clusterDefColl = mongoDB("clusters")


  for (taskName <- taskNames()) createManager(taskName)
  for (clusterName <- clusterNames()) initializeCluster(clusterName)


  def taskNames(): Seq[String] = {
    taskDefColl.find().map(_.as[String]("_id")).toSeq
  }

  def clusterNames(): Seq[String] = {
    clusterDefColl.find().map(_.as[String]("_id")).toSeq
  }

  private def initializeCluster(name: String): ActorRef = {
    val manager = Actors.actorOf(classOf[ClusterManager]).start()
    clusterManagers(name) = manager
    manager ! SetCluster(name, this.self)
    manager
  }


  def createManager(name: String): ActorRef = {
    val manager = Actors.actorOf(classOf[TaskManager]).start()
    taskManagers(name) = manager
    manager ! SetTask(name, this.self)
    manager
  }

  def createCluster(name: String) {
    clusterDefColl += MongoDBObject("_id" -> name)
  }
  protected def receive = {
    receiveListeners orElse {
      case RegisterTaskListener(c) =>
        addListener(c)

      case DeregisterTaskListener(c) =>
        removeListener(c)

      case CreateTask(name) => {
        taskDefColl += MongoDBObject("_id" -> name)
        val manager: ActorRef = createManager(name)
        informListeners(TaskAdded(name, manager))
      }

      case GetCorpusManager(id: String) =>
        val manager = corpusManagers.getOrElseUpdate(id, {
          val actor = Actors.actorOf(classOf[CorpusManager]).start()
          actor ! SetCorpus(id)
          actor
        })
        self.reply(AssignedCorpusManager(manager, id))

      case CreateCluster(name) => {
        createCluster(name)
        val manager: ActorRef = initializeCluster(name)
        informListeners(ClusterAdded(name, manager))
      }

      case GetClusterManager(name) => {
        val manager = clusterManagers.get(name)
        manager match {
          case None =>
            logger.warn("No Manager for " + name)
          case Some(m) =>
            self.reply(AssignedClusterManager(m, name))

        }
      }

      case GetOrCreateClusterManager(name) => {
        val manager = clusterManagers.get(name)
        val created = manager match {
          case None =>
            createCluster(name)
            initializeCluster(name)
          case Some(m) =>
            m
        }
        self.reply(AssignedClusterManager(created, name))
      }

      case GetClusterNames => {
        val names = clusterNames()
        self.reply(ClusterNames(names))
      }


      case GetTaskNames => {
        val names = taskNames()
        self.reply(TaskNames(names))
      }

    }
  }

}


object Mailbox {
  case class LeaveMessage(recipient: String, msg: Any)
  case class RetrieveMessage(recipient: String)
  case class NoSuchMessage(recipient: String)
}

class Mailbox extends Actor with HasLogger {

  private val messages = new HashMap[String, Any]

  protected def receive = {
    case LeaveMessage(r, m) =>
      messages(r) = m
      logger.info("Left Message %s for %s".format(m, r))
    case RetrieveMessage(r) =>
      messages.get(r) match {
        case Some(msg) =>
          messages.remove(r)
          self.reply(msg)
        case None => self.reply(NoSuchMessage(r))
      }

  }
}