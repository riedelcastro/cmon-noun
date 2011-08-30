package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{ActorRef, Actor}

/**
 * @author sriedel
 */
class EntityFeatureExtractionService(val entityMentionAlignmentService: ActorRef,
                                     val mentionService:ActorRef,
                                     val corpusService:ActorRef,
                                     val featureService: ActorRef) extends Actor {

  import EntityFeatureExtractionService._

  protected def receive = {
    case ExtractFeatures(ids) =>
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