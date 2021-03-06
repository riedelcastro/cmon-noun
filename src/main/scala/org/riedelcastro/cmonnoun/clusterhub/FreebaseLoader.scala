package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.Actor._
import java.io.File
import io.Source
import org.riedelcastro.nurupo.{Counting, HasLogger}
import org.riedelcastro.cmonnoun.clusterhub.EntityService.{AddEntity, Entity}
import akka.actor.{Scheduler, ActorRef, Actor}
import java.util.concurrent.TimeUnit

/**
 * @author sriedel
 */
object FreebaseLoader extends HasLogger {

  case class LoadEntities(lines: TraversableOnce[String])

  trait EntityLoading extends DivideAndConquer {

    def em:ActorRef

    type BigJob = LoadEntities
    type SmallJob = LoadEntities
    def unwrapJob = {case l: LoadEntities => l}
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
        val ent = Entity(id, name, freebaseTypes = types)
        em ! AddEntity(ent)
        counting.perform()
      }
    }
  }

  class EntityLoaderMaster2(val em: ActorRef, val numberOfWorkers:Int)
    extends SimpleDivideAndConquerActor with EntityLoading {

  }

  class EntityLoader(val em: ActorRef,val numberOfWorkers:Int)
    extends SimpleDivideAndConquerActor with EntityLoading {

  }



  def main(args: Array[String]) {
    val entityFile = NeoConf.get[File]("freebase-entities")
    val lines = LoadEntities(Source.fromFile(entityFile).getLines())
    val em = Actor.actorOf(new EntityService("freebase")).start()
    val entityLoader = actorOf(new EntityLoader(em,10)).start()
    entityLoader !! lines
    Actor.registry.shutdownAll()


  }


}