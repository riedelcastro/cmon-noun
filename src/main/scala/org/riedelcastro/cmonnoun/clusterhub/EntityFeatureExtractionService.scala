package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{ActorRef, Actor}
import org.riedelcastro.cmonnoun.clusterhub.CorpusService.Sentences
import org.riedelcastro.cmonnoun.clusterhub.EntityMentionService.{ByIds, EntityMention, EntityMentions}

/**
 * @author sriedel
 */
class EntityFeatureExtractionService(val entityMentionAlignmentService: ActorRef,
                                     val mentionService:ActorRef,
                                     val corpusService:ActorRef,
                                     val featureService: ActorRef) extends Actor {

  import EntityFeatureExtractionService._
  import EntityMentionAlignmentService._

  def extract(mention:EntityMention, sentence:CorpusService.Sentence):Set[String] = {
    Set.empty
  }

  protected def receive = {
    case ExtractFeatures(ids) =>
      //todo: this is a crazy way to do a join, maybe easier if alignments store mention specs
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
            val feats = entityMentions.map(m=>extract(m,spec2sentence(m.sentence)))
            val summarized = feats.reduce(_ ++ _)
            FeatureService.NamedFeatures(entityId,summarized)
          }
          featureService ! FeatureService.StoreNamedFeatures(allFeats)
        }
      }
  }
}

object EntityFeatureExtractionService {

  case class ExtractFeatures(entityIds: Stream[Any])

}