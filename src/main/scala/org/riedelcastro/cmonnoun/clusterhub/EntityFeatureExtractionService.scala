package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{ActorRef, Actor}
import org.riedelcastro.cmonnoun.clusterhub.CorpusService.Sentences
import org.riedelcastro.cmonnoun.clusterhub.EntityMentionService.{ByIds, EntityMention, EntityMentions}
import org.riedelcastro.nurupo.HasLogger
import org.riedelcastro.cmonnoun.clusterhub.EntityMentionAlignmentService.EntityIds

/**
 * @author sriedel
 */
class EntityFeatureExtractionService(val entityMentionAlignmentService: ActorRef,
                                     val mentionService: ActorRef,
                                     val corpusService: ActorRef,
                                     val featureService: ActorRef) extends Actor with HasLogger {

  import EntityFeatureExtractionService._
  import EntityMentionAlignmentService._

  def extract(mention: EntityMention, sentence: CorpusService.Sentence): Set[String] = {
    Set("ner:" + mention.ner.getOrElse("N/A"))
  }

  //todo: this is a crazy way to do a join, maybe easier if alignments store mention specs
  def extractFeatures(ids: scala.Stream[Any]) {
    for (Alignments(alignments) <- entityMentionAlignmentService !! GetAlignments(ids);
         mentionIds = alignments.keys;
         EntityMentions(mentions) <- mentionService !! EntityMentionService.Query(ByIds(mentionIds))) {
      //all sentences the mentions are part of
      val specs = mentions.map(_.sentence).toSet

      for (Sentences(sents) <- corpusService !! CorpusService.Query(CorpusService.BySpecs(specs.toStream))) {

        val spec2sentence = sents.map(s => s.sentenceSpec -> s).toMap
        val id2mention = mentions.map(m => m.id.asInstanceOf[Any] -> m).toMap
        val allFeats = for (entityId <- ids) yield {
          val entityMentionIds = mentionIds.filter(alignments(_) == entityId).toSet
          val entityMentions = entityMentionIds.map(id2mention(_))
          val feats = entityMentions.map(m => extract(m, spec2sentence(m.sentence)))
          val summarized = feats.reduceOption(_ ++ _).getOrElse(Set.empty[String])
          FeatureService.NamedFeatures(entityId, summarized)
        }
        featureService ! FeatureService.StoreNamedFeatures(allFeats)
      }
    }
  }
  protected def receive = {
    case ExtractFeatures(ids) =>
      extractFeatures(ids)
      self.channel ! DoneExtracting
  }
}

object EntityFeatureExtractionService extends HasLogger {

  import Actor._

  case class ExtractFeatures(entityIds: Stream[Any])
  case object DoneExtracting

  def main(args: Array[String]) {
    val align = actorOf(new EntityMentionAlignmentService("freebasenyt")).start()
    val mentions = actorOf(new EntityMentionService("nyt")).start()
    val corpus =  actorOf(new CorpusService("nyt")).start()
    val featureStore = actorOf(new BasicFeatureService("entities")).start()
    val entityService = actorOf(new EntityService("freebase")).start()


    val extractor = actorOf(new EntityFeatureExtractionService(align, mentions, corpus, featureStore)).start()

    for (EntityIds(entities) <- align !! EntityMentionAlignmentService.GetEntitiesWithMentions) {
      for (group <- entities.toIterator.grouped(100)) {
        extractor !! ExtractFeatures(group.toStream)
        debugLazy("Processed 100")
      }
    }


    align.stop()
    mentions.stop()
    corpus.stop()
    featureStore.stop()
    entityService.stop()
    extractor.stop()

  }

}