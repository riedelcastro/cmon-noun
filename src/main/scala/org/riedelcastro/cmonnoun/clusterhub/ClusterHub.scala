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

class MutableClusterTask(var name: String) {
  val instances = new ArrayBuffer[Instance]
  val fieldSpecs = new ArrayBuffer[FieldSpec]
}

case class Instance(content: String, fields: Map[String, AnyRef], id: ObjectId = new ObjectId()) {
}

@Salat
trait FieldSpec {
  def name: String
  def extract(instance: String): Any
}

case class SpecHolder(spec: FieldSpec)

case class RegExFieldSpec(name: String, regex: String) extends FieldSpec {
  val r = regex.r

  def extract(instance: String) = {
    r.findFirstIn(instance).isDefined
  }
}

trait MongoSupport {
  def dbName: String = "clusterhub"
  val mongoConn = MongoConnection("localhost", 27017)
  val mongoDB = mongoConn(dbName)
  def collFor(name: String, param: String): MongoCollection = {
    mongoDB(name + "_" + param)
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

  def receiveListeners:PartialFunction[Any,Unit] = {
    case RegisterTaskListener(c) =>
      addListener(c)

    case DeregisterTaskListener(c) =>
      removeListener(c)
  }

}

object ClusterHub {
  case class CreateTask(name: String)
  case class TaskNames(names: Seq[String])
  case class RegisterTaskListener(consumer: ActorRef)
  case class DeregisterTaskListener(consumer: ActorRef)
  case object GetTaskNames
  case class TaskAdded(taskName: String, manager: ActorRef)
  case class GetTaskManager(taskName: String)
  case class AssignedTaskManager(manager: Box[ActorRef])
}


/**
 * @author sriedel
 */
class ClusterHub extends Actor with MongoSupport with HasListeners with HasLogger {

  import ClusterHub._

  private val taskManagers = new HashMap[String, ActorRef]

  private val taskDefColl = mongoDB("tasks")

  for (taskName <- taskNames()) createManager(taskName)

  def taskNames(): Seq[String] = {
    taskDefColl.find().map(_.as[String]("_id")).toSeq
  }

  def createManager(name: String): ActorRef = {
    val manager = Actors.actorOf(classOf[TaskManager]).start()
    taskManagers(name) = manager
    manager ! SetTask(name, this.self)
    manager
  }

  protected def receive = {
    case RegisterTaskListener(c) =>
      addListener(c)

    case DeregisterTaskListener(c) =>
      removeListener(c)

    case CreateTask(name) => {
      taskDefColl += MongoDBObject("_id" -> name)
      val manager: ActorRef = createManager(name)
      informListeners(TaskAdded(name, manager))
    }

    case GetTaskManager(name) => {
      val manager = taskManagers.get(name)
      self.reply(AssignedTaskManager(manager))
      manager match {
        case None => logger.warn("No Manager for " + name)
        case _ =>
      }
    }

    case GetTaskNames => {
      val names = taskNames
      self.reply(TaskNames(names))
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