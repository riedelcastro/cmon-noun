package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{ActorRef, Actor}
import collection.mutable.{HashSet, ArrayBuffer}

case class CreateProblem(name:String)
case class ProblemNames(names:Seq[String])
case class RegisterProblemListConsumer(consumer:ActorRef)
case class DeregisterProblemListConsumer(consumer:ActorRef)

class Problem(var name:String) {
  var instances = new ArrayBuffer[String]
}

/**
 * @author sriedel
 */
class ProblemManager extends Actor {

  val problems = new ArrayBuffer[Problem]
  val problemListConsumers = new HashSet[ActorRef]

  protected def receive = {
    case RegisterProblemListConsumer(c) =>
      problemListConsumers += c
    case DeregisterProblemListConsumer(c) =>
      problemListConsumers -= c
    case CreateProblem(name) => {
      problems += new Problem(name)
      val names = problems.map(_.name)
      for (consumer <- problemListConsumers){
        consumer ! ProblemNames(names)
      }
    }
  }

}
