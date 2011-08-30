package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.Actor._
import akka.routing._
import akka.routing.Routing
import org.riedelcastro.cmonnoun.clusterhub.DivideAndConquerActor.BigJobDone
import org.riedelcastro.nurupo.HasLogger
import akka.actor.{ActorRef, PoisonPill, Actor}

/**
 * Divides a big job into small jobs and distributes these among a set of workers.
 *
 * @author sriedel
 */
trait DivideAndConquerActor extends Actor with HasListeners with HasLogger {

  type BigJob
  type SmallJob

  case object Done

  trait Worker extends Actor {
    def doYourJob(job: SmallJob)

    protected def receive = {
      case job =>
        doYourJob(job.asInstanceOf[SmallJob])
        self.channel ! Done
    }
  }


  def divide(bigJob: BigJob): Iterator[SmallJob]

  def unwrapJob: PartialFunction[Any, BigJob]

  protected def receive = {
    unwrapJob.andThen {
      job =>
        for (smallJob <- divide(job)) {
          count += 1
          router ! smallJob
          if (count % 100 == 0)
            infoLazy("Started %d sub-tasks".format(count))
        }
    } orElse receiveListeners orElse {
      case Done =>
        count -= 1
        if (count % 100 == 0)
          infoLazy("%d sub-tasks remaining".format(count))
        if (count == 0) {
          informListeners(BigJobDone(bigJobName))
        }
    }
  }

  var count = 0

  def newWorker(): Worker

  def numberOfWorkers: Int

  def bigJobName: String = "bigJob"

  val workers = List.fill(numberOfWorkers)(actorOf(newWorker()).start())
  val router = Routing.loadBalancerActor(new CyclicIterator(workers))

  override def postStop() {
    router ! Routing.Broadcast(PoisonPill)
    router ! PoisonPill
    infoLazy("Stopped divide and conquer actor")
  }

}

trait SimpleDivideAndConquerActor extends DivideAndConquerActor {

  def smallJob(job: SmallJob)

  def newWorker() = new Worker {
    def doYourJob(job: SmallJob) {
      smallJob(job)
    }
  }

}


object DivideAndConquerActor {
  case class BigJobDone(id: String)

  class BigJobDoneHook(val hook: () => Unit) extends Actor with HasLogger {
    protected def receive = {
      case BigJobDone(_) =>
        hook()
        infoLazy("BigJobDoneHook called")
        self.stop()
    }
  }
  def bigJobDoneHook(divideAndConquerActor: ActorRef)(hook: () => Unit) {
    val actor = actorOf(new BigJobDoneHook(hook)).start()
    divideAndConquerActor ! HasListeners.RegisterListener(actor)
  }

}

trait StopWhenMailboxEmpty {
  this: Actor =>

  def stopWhenMailboxEmpty: Receive = {
    case StopWhenMailboxEmpty =>
      if (self.mailboxSize == 0)
        self ! PoisonPill
  }

}

case object StopWhenMailboxEmpty