package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{ActorRef, Actor}
import org.riedelcastro.cmonnoun.clusterhub.CorpusService.Sentences
import org.riedelcastro.cmonnoun.clusterhub.EntityMentionService.{EntityMention, EntityMentions}

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
      for (EntityMentions(entityMentions) <- entityMentionAlignmentService !! GetMentions(mentionService,ids)) {
        val specs = entityMentions.map(_.sentence).toSet
        for (Sentences(sents) <- corpusService !! CorpusService.Query()) {
          val spec2sentence = sents.map(s => s.sentenceSpec -> s).toMap
          val perSentenceFeats = entityMentions.map(mention => extract(mention,spec2sentence(mention.sentence)))
          val summarized = perSentenceFeats.reduce(_ ++ _)
        }
      }
      //for (MentionIds(ids) <- entityMentionAlignmentService !! EntityMentionAlignmentService.GetMentionIds(ids);
      //     Mentions(mentions) <- mentionService !! EntityMentionService.GetEntityMentions(ids);
      //     specs = sentenceSpecs(mentions);
      //     Sentences(sents) <- corpusService !! CorpusManager.GetSentences(specs))
      //val mentionIds = entityMentionAlignmentService !! EntityMentionAlignmentService.GetMentionIds(ids)
      //val mentions = mentionService !! EntityMentionService.GetEntityMentions(mentionIds)
      //val sentences = corpusService !! CorpusManager.GetSentences(mentions)
      //val featuresAsStrings = extract(...)
      //self.channel ! FeaturesAsStrings(featuresAsStrings)
  }
}

object EntityFeatureExtractionService {

  case class ExtractFeatures(entityIds: Stream[Any])

}