package org.riedelcastro.cmonnoun.clusterhub

import org.riedelcastro.cmonnoun.clusterhub.EntityService.ByName
import akka.actor.{Actor, ActorRef}

/**
 * Loads sentences from corpus, extracts mentions, and sends
 * these to a entity mention service.
 *
 * @author sriedel
 */
class EntityMentionExtractionService(entityMentionService: ActorRef)
  extends DivideAndConquerActor {

  val models = new EntityMentionModels

  type BigJob = CorpusService.Sentences
  type SmallJob = CorpusService.Sentences


  def unwrapJob = {
    case job: CorpusService.Sentences => job
  }

  def numberOfWorkers = 10

  def divide(bigJob: CorpusService.Sentences) = {
    for (group <- bigJob.sentences.toIterator.grouped(100)) yield CorpusService.Sentences(group)
  }

  def newWorker() = new PerDocExtractor

  class PerDocExtractor extends Worker {
    val extractor = new EntityMentionExtractor(models)

    def doYourJob(job: SmallJob) {
      for (sent <- job.sentences) {
        val mentions = extractor.extractMentions(sent)
        for (mention <- mentions) {
          entityMentionService ! EntityMentionService.StoreEntityMention(mention)
        }
      }
    }
  }

}


object EntityMentionExtractionService {

  import Actor._

  case object SentencesProcessed

  def main(args: Array[String]) {
    val corpus = actorOf(new CorpusService("nyt")).start()
    val mentionService = actorOf(new EntityMentionService("nyt")).start()
    val extractor = actorOf(new EntityMentionExtractionService(mentionService)).start()
    //make sure we stop everything after we are done with extraction
    DivideAndConquerActor.bigJobDoneHook(extractor) {
      () =>
        extractor.stop()
        corpus.stop()
        mentionService.stop()
    }

    //get sentences to extractor
    corpus !! CorpusService.SentenceQuery("") match {
      case Some(s: CorpusService.Sentences) => extractor ! s
      case None => {}
    }


  }
}

/**
 * Receives mentions, finds entities that match, stores these to the given alignment service.
 */
abstract class EntityMentionAlignmentExtractionService(val entityService: ActorRef, val alignmentService: ActorRef)
  extends DivideAndConquerActor {

  import EntityMentionService._

  override def bigJobName = "alignmentExtractionService"
  type BigJob = EntityMentions
  type SmallJob = EntityMentions
  def unwrapJob = {case job: EntityMentions => job}
  def divide(bigJob: EntityMentions) = for (group <- bigJob.mentions.toIterator.grouped(100)) yield
    EntityMentions(group)
  def numberOfWorkers = 10


  class ExtractionWorker extends Worker {
    def doYourJob(job: EntityMentions) {
      for (mention <- job.mentions) {
        //todo: avoid blocking
        entityService !! EntityService.Query(ByName(mention.phrase)) match {
          case Some(EntityService.Entities(entities)) =>
            for (entity <- entities.toStream.headOption)
              alignmentService ! EntityMentionAlignmentService.StoreAlignment(mention.id, entity.id)
          case _ =>
        }
      }
    }
  }

}

object EntityMentionAlignmentExtractionService {

}