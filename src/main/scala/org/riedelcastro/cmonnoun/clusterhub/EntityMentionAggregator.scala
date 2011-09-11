package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.ActorRef

/**
 * @author sriedel
 */
trait EntityMentionAggregator {



  def entityMentionService:ActorRef



}