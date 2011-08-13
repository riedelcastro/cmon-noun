package org.riedelcastro.cmonnoun.clusterhub

import com.novus.salat.annotations
import annotations.raw.Salat
import com.mongodb.casbah.Imports._
import net.liftweb.common.{Full, Empty, Box}
import Box._
import collection.mutable.{HashMap, HashSet, ArrayBuffer}
import akka.actor.{Actors, ActorRef, Actor}


class MutableClusterTask(var name: String) {
  val instances = new ArrayBuffer[Instance]
  val fieldSpecs = new ArrayBuffer[FieldSpec]
}

case class Instance(content: String, fields: Map[String, Any])

@Salat
trait FieldSpec {
  type T
  def name: String
  def extract(instance: String): T
}

case class RegExFieldSpec(name: String, regex: String) extends FieldSpec {
  val r = regex.r
  type T = Boolean

  def extract(instance: String) = {
    r.findFirstIn(instance).isDefined
  }
}

trait MongoSupport {
  def dbName: String = "clusterhub"
  val mongoConn = MongoConnection("locahost", 27017)
  val mongoDB = mongoConn(dbName)
  def collFor(name: String, param: String): MongoCollection = {
    mongoDB(name + "_" + param)
  }
}

trait HasListeners {
  private val taskListeners = new HashSet[ActorRef]

  def informListeners(msg: Any) {
    for (l <- taskListeners) l ! msg

  }

  def addListener(l:ActorRef) {
    taskListeners += l
  }

  def removeLister(l:ActorRef) {
    taskListeners -= l
  }

}

object ClusterHub {
  case class CreateTask(name: String)
  case class TaskNames(names: Seq[String])
  case class RegisterTaskListener(consumer: ActorRef)
  case class DeregisterTaskListener(consumer: ActorRef)
  case object GetTaskNames
  case class TaskAdded(taskName: String, manager: ActorRef)
  case class GetTaskManager(taskName:String)
  case class AssignedTaskManager(manager:Box[ActorRef])
}


/**
 * @author sriedel
 */
class ClusterHub extends Actor with MongoSupport with HasListeners {

  import ClusterHub._

  private val taskManagers = new HashMap[String, ActorRef]

  private val taskDefColl = mongoDB("tasks")

  protected def receive = {
    case RegisterTaskListener(c) =>
      addListener(c)

    case DeregisterTaskListener(c) =>
      removeLister(c)

    case CreateTask(name) => {
      taskDefColl += MongoDBObject("_id" -> name)
      val manager = Actors.actorOf(classOf[TaskManager]).start()
      taskManagers(name) = manager
      informListeners(TaskAdded(name, manager))
    }

    case GetTaskManager(name) => {
      self.reply(AssignedTaskManager(taskManagers.get(name)))
    }

    case GetTaskNames => {
      val names = taskDefColl.find().map(_.as[String]("_id")).toSeq
      self.reply(TaskNames(names))
    }

  }

}

object TaskManager {
  case class SetTask(taskName: String, hub: ActorRef)
  case object GetInstances
  case class Instances(instances: Seq[Instance])
  case class AddInstance(instance: String)
  case class AddField(field: FieldSpec)
  case class InstanceAdded(taskName: String, instance: Instance)

}

class TaskManager extends Actor with MongoSupport with HasListeners {

  import TaskManager._

  private var taskName: Box[String] = Empty
  private var hub: Box[ActorRef] = Empty

  private def getInstances(task: String): MongoCollection = {
    collFor("instances", task)
  }
  protected def receive = {
    case SetTask(n, h) =>
      taskName = Full(n)
      hub = Full(h)

    case GetInstances =>
      for (n <- taskName) {
        val coll = getInstances(n)
        val instances = coll.find().map(dbo => {
          val content = dbo.as[String]("field")
          Instance(content, Map.empty)
        })
        self.reply(Instances(instances.toSeq))
      }

    case AddInstance(instance: String) => {
      for (n <- taskName) {
        val coll = getInstances(n)
        coll += MongoDBObject("content" -> instance)
        informListeners(InstanceAdded(n, Instance(instance, Map.empty)))
      }
    }

  }
}