package org.riedelcastro.cmonnoun.clusterhub

import com.novus.salat.annotations
import annotations.raw.Salat
import com.mongodb.casbah.Imports._
import net.liftweb.common.{Full, Empty, Box}
import Box._
import collection.mutable.{HashMap, HashSet, ArrayBuffer}
import akka.actor.{Actors, ActorRef, Actor}
import org.riedelcastro.cmonnoun.clusterhub.Mailbox.{NoSuchMessage, RetrieveMessage, LeaveMessage}
import org.riedelcastro.nurupo.HasLogger
import org.riedelcastro.cmonnoun.clusterhub.TaskManager.SetTask
import org.riedelcastro.cmonnoun.clusterhub.ClusterHub.{DeregisterTaskListener, RegisterTaskListener}

import com.novus.salat._
import com.novus.salat.global._
import org.bson.types.ObjectId
import com.mongodb.casbah.commons.MongoDBObject

/**
 * @author sriedel
 */
object TaskManager {

  trait TaskChanged

  case class SetTask(taskName: String, hub: ActorRef)

  case object GetInstances
  case class Instances(instances: Seq[Instance])

  case class InstanceAdded(taskName: String, instance: Instance) extends TaskChanged
  case class FieldSpecAdded(spec: FieldSpec) extends TaskChanged

  case class AddCluster(clusterId:String)
  case class AddInstance(content: String)
  case class AddField(field: FieldSpec)

}

class TaskManager extends Actor with MongoSupport with HasListeners with HasLogger {

  import TaskManager._

  private var taskName: Box[String] = Empty
  private var hub: Box[ActorRef] = Empty

  private val fieldSpecs = new ArrayBuffer[FieldSpec]

  private def getInstancesColl(task: String): MongoCollection = {
    collFor("instances", task)
  }

  private def loadFieldSpecs() {
    for (n <- taskName){
      val coll = fieldSpecColl(n)
      for (dbo <- coll.find()) {
        val name = dbo.as[String]("name")
        val spec = dbo.as[String]("type") match {
          case "regex" => Some(RegExFieldSpec(name,dbo.as[String]("regex")))
          case _ => None
        }
        for (s <- spec) fieldSpecs += s
      }
    }
  }

  private def evaluateFieldValues(spec: FieldSpec) {
    for (n <- taskName) {
      val coll = getInstancesColl(n)
      coll.find().foreach(dbo => {
        for (id <- dbo._id) {
          val content = dbo.as[String]("content")
          val result = spec.extract(content)
          val wr = coll.update(
            MongoDBObject("_id" -> id),
            MongoDBObject("$set" -> MongoDBObject(spec.name -> result)),false,true)
          debugLazy("Update after field added: " + wr.getLastError.getErrorMessage)
        }
      })
    }
  }

  private def getInstances(name: String) = {
    val coll = getInstancesColl(name)
    val instances = coll.find().map(dbo => {
      val content = dbo.as[String]("content")
      val fields = dbo.filterKeys(k => k != "content" && k != "_id").toMap
      Instance(content, fields, dbo._id.get)
    })
    instances
  }

  def fieldSpecColl(n: String): MongoCollection = {
    collFor("fieldSpecs", n)
  }

  protected def receive = {
    case SetTask(n, h) =>
      taskName = Full(n)
      hub = Full(h)
      loadFieldSpecs()


    case GetInstances =>
      for (n <- taskName) {
        val instances = getInstances(n)
        self.reply(Instances(instances.toSeq))
      }

    case AddInstance(content) => {
      logger.debug("Received instance " + content)
      for (n <- taskName) {
        val coll = getInstancesColl(n)
        val fields = fieldSpecs.map(s => s.name -> s.extract(content).asInstanceOf[AnyRef])
        val instance = Instance(content,fields.toMap)
        coll += MongoDBObject(List("_id" -> instance.id, "content" -> instance.content) ++ fields.toList)
        informListeners(InstanceAdded(n, instance))
      }
    }

    case AddField(field) => {
      for (n <- taskName) {
        fieldSpecs += field
        val coll = fieldSpecColl(n)
        field match {
          case RegExFieldSpec(name, regex) =>
            coll += MongoDBObject("type" -> "regex", "name" -> name, "regex" -> regex)
            evaluateFieldValues(field)
            informListeners(FieldSpecAdded(field))
        }
      }
    }

    case RegisterTaskListener(c) =>
      addListener(c)

    case DeregisterTaskListener(c) =>
      removeListener(c)

  }
}