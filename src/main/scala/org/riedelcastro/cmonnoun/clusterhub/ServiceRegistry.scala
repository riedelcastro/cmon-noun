package org.riedelcastro.cmonnoun.clusterhub

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.MongoCollection
import org.riedelcastro.cmonnoun.clusterhub.EntityService._
import com.mongodb.casbah.Imports._
import collection.mutable.HashMap
import akka.actor.{ActorRef, Actor}

/**
 * @author sriedel
 */
object ServiceRegistry {
  case class GetOrCreateService(name: String)
  case object GetServices
  case class Service(name: String, service: ActorRef)
  case class Services(services: Seq[Service])
}


trait ServiceRegistry extends Actor with MongoSupport {

  import ServiceRegistry._

  loadServicesFromStore()

  def registryName: String
  def create(name: String): Actor
  private lazy val services = new HashMap[String, ActorRef]
  private lazy val coll = collFor("services", registryName)


  def initializeService(name: String): ActorRef = {
    val service = Actor.actorOf(create(name)).start()
    coll += MongoDBObject("_id" -> name)
    service

  }

  private def loadServicesFromStore() {
    for (dbo <- coll.find()) {
      val name = dbo.as[String]("_id")
      val service = Actor.actorOf(create(name)).start()
      services(name) = service
    }
  }

  protected def receive = {
    case GetOrCreateService(name) =>
      val service = services.getOrElseUpdate(name, initializeService(name))
      self.reply(Service(name, service))

    case GetServices =>
      val result = services.map({case (name, service) => Service(name, service)}).toSeq
      self.reply(Services(result))

  }
}









