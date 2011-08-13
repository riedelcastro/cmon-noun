package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{ActorRef, Actor}
import com.novus.salat.annotations
import annotations.raw.Salat
import com.mongodb.casbah.Imports._
import collection.mutable.{HashSet, ArrayBuffer}

case class CreateTask(name: String)
case class TaskNames(names: Seq[String])
case class RegisterTaskListener(consumer: ActorRef)
case class DeregisterTaskListener(consumer: ActorRef)
case class GetInstances(name: String)
case object GetTaskNames
case object TaskListChanged
case class AddInstance(problemName: String, instance: String)
case class AddField(problemName:String, field:FieldSpec)
case class Instances(instances: Seq[Instance])
case class InstanceAdded(taskName:String, instance:Instance)


class MutableClusterTask(var name:String) {
  val instances = new ArrayBuffer[Instance]
  val fieldSpecs = new ArrayBuffer[FieldSpec]
}

case class Instance(content:String, fields:Map[String,Any])

@Salat
trait FieldSpec {
  type T
  def name:String
  def extract(instance:String):T
}

case class RegExFieldSpec(name:String, regex:String) extends FieldSpec {
  val r = regex.r
  type T = Boolean

  def extract(instance: String) = {
    r.findFirstIn(instance).isDefined
  }
}

/**
 * @author sriedel
 */
class ClusterHub extends Actor {

  private val taskListeners = new HashSet[ActorRef]

  private val mongoConn = MongoConnection("locahost", 27017)
  private val mongoDB = mongoConn("clusterhub")
  private val taskDefColl = mongoDB("tasks")

  def collForTask(name: String): MongoCollection = {
    mongoDB("task_" + name)
  }

  def informListeners(msg:Any) {
    for (l <- taskListeners) l ! msg

  }

  protected def receive = {
    case RegisterTaskListener(c) =>
      taskListeners += c

    case DeregisterTaskListener(c) =>
      taskListeners -= c

    case CreateTask(name) => {
      taskDefColl += MongoDBObject("_id" -> name)
      informListeners(TaskListChanged)
    }

    case GetTaskNames => {
      val names = taskDefColl.find(MongoDBObject.empty,MongoDBObject("_id" -> 1)).map(_.as[String]("_id")).toSeq
      self.reply(TaskNames(names))
    }

    case GetInstances(name) => {
      val coll = collForTask(name)
      val instances = coll.find().map(dbo => {
        val content = dbo.as[String]("field")
        Instance(content,Map.empty)
      })
      self.reply(Instances(instances.toSeq))
    }

    case AddInstance(name: String, instance: String) => {
      val coll = collForTask(name)
      coll += MongoDBObject("content" -> instance)
      informListeners(InstanceAdded(name,Instance(instance,Map.empty)))
    }
  }

}
