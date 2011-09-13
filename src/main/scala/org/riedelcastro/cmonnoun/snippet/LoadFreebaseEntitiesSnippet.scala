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
import org.riedelcastro.cmonnoun.comet.ProgressMonitorComet
import org.riedelcastro.nurupo.Util
import org.riedelcastro.cmonnoun.clusterhub.NYTSentenceLoader.{Files, DocLoaderMaster}
import org.riedelcastro.cmonnoun.clusterhub.EntityMentionService.EntityMentions
import org.riedelcastro.cmonnoun.clusterhub.EntityMentionService.EntityMentions._
import net.liftweb.http.{SessionVar, LiftSession, S, SHtml}

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
          session.sendCometActorMessage("ProgressMonitorComet", Full("freebase_load_monitor"), SetProgressMonitor(m))
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

class LoadNYTSnippet {

  def render = {
    var dest: String = "nyt"
    var prefix: String = "2007/01/01"
    var workers: Int = 5

    def load() {
      import Actor._
      import ProgressMonitorComet._
      import NYTSentenceLoader._
      import DivideAndConquerActor._
      val cm = actorOf(new CorpusService(dest)).start()
      val docLoader = actorOf(new DocLoaderMaster(cm)).start()
      val nytDir = NeoConf.get[File]("nyt")
      val nytPrefix = new File(nytDir, prefix)
      val files = if (nytPrefix.isDirectory) Util.files(nytPrefix)
      else {
        val dir = nytPrefix.getParentFile
        val prefix = nytPrefix.getName
        Util.files(dir).filter(_.getName.startsWith(prefix))
      }

      for (DivideAndConquerActor.GetProgressMonitorResult(m) <- docLoader !! GetProgressMonitor) {
        //        actorOf(new Finalizer(m, () => {em.stop(); loader.stop()})).start()
        for (session <- S.session) {
          session.sendCometActorMessage("ProgressMonitorComet", Full("nyt_load_monitor"), SetProgressMonitor(m))
        }
      }
      docLoader ! Files(files)
    }

    val result = Seq(
      "#load_nyt_prefix" #> SHtml.text(prefix, prefix = _),
      "#load_nyt_dest" #> SHtml.text(dest, dest = _),
      "#load_nyt_submit" #> SHtml.submit("Start", () => load()),
      "#load_nyt_workers" #> SHtml.number(workers, workers = _, 1, 10)
    ).reduce(_ & _)
    result

  }


}

class ExtractMentionsSnippet {

  def render = {
    var dest: String = "nyt_mentions"
    var source: String = "nyt"
    var workers: Int = 5

    def load() {
      import Actor._
      import ProgressMonitorComet._
      import DivideAndConquerActor._
      import CorpusService._

      val corpus = actorOf(new CorpusService(source)).start()
      val mentionService = actorOf(new EntityMentionService(dest)).start()
      val extractor = actorOf(new EntityMentionExtractionService(mentionService)).start()

      for (DivideAndConquerActor.GetProgressMonitorResult(m) <- extractor !! GetProgressMonitor) {
        //        actorOf(new Finalizer(m, () => {em.stop(); loader.stop()})).start()
        for (session <- S.session) {
          session.sendCometActorMessage("ProgressMonitorComet", Full("extract_mention_monitor"), SetProgressMonitor(m))
        }
      }

      //sends an ALL query to the corpus, which will send sentences to the extractor
      corpus.sendOneWay(Query(),extractor)

    }

    val result = Seq(
      "#extract_mention_source" #> SHtml.text(source, source = _),
      "#extract_mention_dest" #> SHtml.text(dest, dest = _),
      "#extract_mention_submit" #> SHtml.submit("Start", () => load()),
      "#extract_mention_workers" #> SHtml.number(workers, workers = _, 1, 10)
    ).reduce(_ & _)
    result

  }

}

class AlignMentionsSnippet {

  object sourceEntities extends SessionVar[String]("freebase")
  object sourceMentions extends SessionVar[String]("nyt")
  object dest extends SessionVar[String]("nyt_freebase")
  object workers extends SessionVar[Int](5)


  def render = {

    def load() {
      import Actor._
      import ProgressMonitorComet._
      import DivideAndConquerActor._

      val entityService = actorOf(new EntityService(sourceEntities.is)).start()
      val alignmentService = actorOf(new EntityMentionAlignmentService(dest.is)).start()
      val entityMentionService = actorOf(new EntityMentionService(sourceMentions.is)).start()
      val aligner = actorOf(new EntityMentionAlignerService(entityService,alignmentService)).start()

      for (DivideAndConquerActor.GetProgressMonitorResult(m) <- aligner !! GetProgressMonitor) {
        for (session <- S.session) {
          session.sendCometActorMessage("ProgressMonitorComet", Full("align_mention_monitor"), SetProgressMonitor(m))
        }
      }

      entityMentionService.sendOneWay(EntityMentionService.Query(), aligner)

    }

    val result = Seq(
      "#align_mention_source_entities" #> SHtml.text(sourceEntities, sourceEntities(_)),
      "#align_mention_source_mentions" #> SHtml.text(sourceMentions, sourceMentions(_)),
      "#align_mention_dest" #> SHtml.text(sourceMentions, sourceMentions(_)),
      "#align_mention_submit" #> SHtml.submit("Start", () => load()),
      "#align_mention_workers" #> SHtml.number(workers, workers(_), 1, 10)
    ).reduce(_ & _)
    result

  }

}