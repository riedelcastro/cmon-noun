package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.Actor._
import java.io.File
import io.Source
import org.riedelcastro.nurupo.{Counting, HasLogger}
import akka.actor.{ActorRef, Actor}
import org.riedelcastro.cmonnoun.clusterhub.DivideAndConquerActor.BigJobDone
import org.riedelcastro.cmonnoun.clusterhub.EntityService.{EntityAdded, AddEntity, Entity}

/**
 * @author sriedel
 */
object FreebaseLoader extends HasLogger {

  case class LoadEntities(lines:TraversableOnce[String])

  class EntityLoaderMaster2(val em:ActorRef) extends SimpleDivideAndConquerActor {
    type BigJob = LoadEntities
    type SmallJob = LoadEntities
    def numberOfWorkers = 10
    def unwrapJob = {case l:LoadEntities => l}
    def divide(bigJob: LoadEntities) = for (group <- bigJob.lines.toIterator.grouped(10000)) yield
      LoadEntities(group)

    def smallJob(job: LoadEntities) {
      val lines = job.lines
      val counting = new Counting(1000, c => infoLazy("Processed %d lines".format(c)))
      for (line <- lines) {
        val split = line.split("\t")
        val id = split(0)
        val name = split(1)
        val types = split(2).split(",")
        val ent = Entity(id,name,freebaseTypes = types)
        em ! AddEntity(ent)
        counting.perform()
      }
    }
  }


  def main(args: Array[String]) {
    val entityFile = NeoConf.get[File]("freebase-entities")
    val lines = LoadEntities(Source.fromFile(entityFile).getLines().take(100))
    val em = Actor.actorOf(new EntityService("freebase")).start()
    val entityLoader = actorOf(new EntityLoaderMaster2(em)).start()
    class Cleaner extends Actor {
      var count = 100
      protected def receive = {
        case BigJobDone(_) =>
          entityLoader.stop()
        case EntityAdded(_) =>
          count -= 1
          if (count == 0) {
            self.stop()
            em.stop()
          }
      }
    }

    val cleaner = actorOf(new Cleaner).start()

    em ! HasListeners.RegisterListener(cleaner)
    entityLoader ! HasListeners.RegisterListener(cleaner)
    entityLoader ! lines



  }


}