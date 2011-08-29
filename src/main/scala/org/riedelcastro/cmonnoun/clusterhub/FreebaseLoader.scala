package org.riedelcastro.cmonnoun.clusterhub

import org.riedelcastro.cmonnoun.clusterhub.FreebaseLoader.LoadEntities
import akka.actor.Actor._
import akka.routing.{CyclicIterator, Routing}
import java.io.File
import io.Source
import akka.actor.{PoisonPill, Actor}
import org.riedelcastro.cmonnoun.clusterhub.EntityManager.{AddEntity, Entity}
import org.riedelcastro.nurupo.{Counting, HasLogger}

/**
 * @author sriedel
 */
object FreebaseLoader extends HasLogger {

  case class LoadEntities(lines:TraversableOnce[String])
  case object Done
  case object Start

  val em = Actor.actorOf(new EntityManager("freebase")).start()

  class EntityLoader extends Actor {
    protected def receive = {
      case LoadEntities(lines)=>
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
        self.channel ! Done
    }
  }

  class EntityLoaderMaster(val file:File) extends Actor {

    val loaders = List.fill(10)(actorOf(new EntityLoader).start())
    val router = Routing.loadBalancerActor(new CyclicIterator(loaders)).start()

    var count = 0

    protected def receive = {
      case Start =>
        val source = Source.fromFile(file)
        for (lines <- source.getLines().grouped(10000)) {
          count += 1
          router ! LoadEntities(lines)
          infoLazy("Added to process: " + count)
        }
      case Done =>
        count -= 1
        infoLazy("Left to process: " + count)
        if (count == 0) {
          router ! Routing.Broadcast(PoisonPill)
          router ! PoisonPill
          em ! PoisonPill
          self ! PoisonPill
        }
    }
  }

  def main(args: Array[String]) {
    val entityFile = new File("/Users/riedel/corpora/freebase/manual-nyt-entities.tsv")
    val entityLoader = actorOf(new EntityLoaderMaster(entityFile)).start()
    entityLoader ! Start
  }


}