package org.riedelcastro.cmonnoun.snippet

import net.liftweb.common._
import net.liftweb.util._
import Helpers._
import java.io.File
import org.riedelcastro.cmonnoun.clusterhub.FreebaseLoader.LoadEntities._
import io.Source
import org.riedelcastro.cmonnoun.clusterhub.FreebaseLoader.{EntityLoader, LoadEntities}
import akka.actor.Actor
import org.riedelcastro.cmonnoun.clusterhub._
import net.liftweb.http.{LiftSession, S, SHtml}
import org.riedelcastro.cmonnoun.comet.ProgressMonitorComet

/**
 * @author sriedel
 */
class LoadFreebaseEntitiesSnippet {

  def render = {
    var dest: String = "freebase"
    var workers: Int = 5

    def load() {
      import Actor._
      import ProgressMonitorComet._
      val entityFile = NeoConf.get[File]("freebase-entities")
      val lines = LoadEntities(Source.fromFile(entityFile).getLines())
      val em = actorOf(new EntityService(dest)).start()
      val loader = actorOf(new EntityLoader(em, workers)).start()

      for (DivideAndConquerActor.GetProgressMonitorResult(m) <- loader !! DivideAndConquerActor.GetProgressMonitor) {
//        actorOf(new Finalizer(m, () => {em.stop(); loader.stop()})).start()
        for (session <- S.session) {
          session.sendCometActorMessage("ProgressMonitorComet",Full("freebase_load_monitor"),SetProgressMonitor(m))
        }
      }
      loader ! lines
    }

    val result = Seq(
      "#load_entities_dest" #> SHtml.text(dest, dest = _),
      "#load_entities_submit" #> SHtml.submit("Start", () => load()),
      "#load_entities_workers" #> SHtml.number(workers, workers = _, 1, 10)
    ).reduce(_ & _)
    result

  }


}