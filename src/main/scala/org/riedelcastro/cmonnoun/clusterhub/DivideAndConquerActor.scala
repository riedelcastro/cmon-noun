package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.Actor._
import akka.routing._
import akka.routing.Routing
import org.riedelcastro.nurupo.HasLogger
import akka.actor.{ActorRef, PoisonPill, Actor}
import org.riedelcastro.cmonnoun.clusterhub.ProgressMonitor.SetProblemSize
import org.riedelcastro.cmonnoun.clusterhub.DivideAndConquerActor.{GetProgressMonitorResult, GetProgressMonitor, BigJobDone}

trait DivideAndConquer extends Actor {

  type BigJob
  type SmallJob

  case object Done

  val progressMonitor: ActorRef = actorOf(new ProgressMonitor).start()

  trait Worker extends Actor {
    def doYourJob(job: SmallJob)

    protected def receive = {
      case job =>
        doYourJob(job.asInstanceOf[SmallJob])
        progressMonitor ! ProgressMonitor.TaskSolved
        self.channel ! Done
    }
  }


  def divide(bigJob: BigJob): Iterator[SmallJob]

  def unwrapJob: PartialFunction[Any, BigJob]

  def newWorker(): Worker

  def numberOfWorkers: Int

  def bigJobName: String = "bigJob"

  def receiveGetMonitor:Receive = {
    case GetProgressMonitor =>
      self.reply(GetProgressMonitorResult(progressMonitor))
  }

  override def postStop() {
    progressMonitor.stop()
  }
}


/**
 * Divides a big job into small jobs and distributes these among a set of workers.
 *
 * @author sriedel
 */
trait DivideAndConquerActor extends Actor with DivideAndConquer with HasListeners with HasLogger {


  protected def receive = {
    unwrapJob.andThen {
      job =>
        val smallJobs = divide(job)
        if (smallJobs.isEmpty)
          informListeners(BigJobDone(bigJobName))
        else {
          for (smallJob <- smallJobs) {
            count += 1
            router ! smallJob
            if (count % 100 == 0)
              infoLazy("Started %d sub-tasks".format(count))
            progressMonitor ! ProgressMonitor.SetProblemSize(count)
          }
        }

    } orElse receiveListeners orElse receiveGetMonitor orElse {
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

  val workers = List.fill(numberOfWorkers)(actorOf(newWorker()).start())
  val router = Routing.loadBalancerActor(new CyclicIterator(workers))

  override def postStop() {
    super.postStop()
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

trait SimpleSyncedDivideAndConquerActor extends SyncedDivideAndConquerActor {

  def smallJob(job: SmallJob)

  def newWorker() = new Worker {
    def doYourJob(job: SmallJob) {
      smallJob(job)
    }
  }

}


object DivideAndConquerActor {
  case class BigJobDone(id: String)

  case object GetProgressMonitor
  case class GetProgressMonitorResult(progressMonitor:ActorRef)

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

trait PrioritizedActor extends Actor {
  def lowPriority: Receive
  def highPriority: Receive

  protected def receive = {
    highPriority orElse lowPriority
  }
}

trait SyncedDivideAndConquerActor extends Actor with DivideAndConquer with HasListeners with HasLogger {


  protected def receive = {
    unwrapJob.andThen {
      job =>
        val smallJobs = divide(job)
        if (!smallJobs.isEmpty) {
          val futures = for (smallJob <- smallJobs) yield {
            count += 1
            if (count % 100 == 0)
              infoLazy("Started %d sub-tasks".format(count))
            val future = router !!! smallJob
            progressMonitor ! ProgressMonitor.SetProblemSize(count)
            future
          }
          for (future <- futures) future.await
        }
        informListeners(BigJobDone(bigJobName))
        self.channel ! BigJobDone(bigJobName)

    } orElse receiveListeners orElse receiveGetMonitor
  }

  var count = 0

  val workers = List.fill(numberOfWorkers)(actorOf(newWorker()).start())
  val router = Routing.loadBalancerActor(new CyclicIterator(workers))

  override def postStop() {
    super.postStop()
    router ! Routing.Broadcast(PoisonPill)
    router ! PoisonPill
    infoLazy("Stopped divide and conquer actor")
  }

}

class ProgressMonitor extends Actor with HasListeners {
  var problemSize = 1
  var solved = 0
  import ProgressMonitor._
  protected def receive = {
    receiveListeners orElse {
      case SetProblemSize(size) =>
        problemSize = size
      case TaskSolved =>
        solved += 1
        informListeners(ProgressMonitor.Progress(solved,problemSize))
    }

  }
}

object ProgressMonitor {

  case object TaskSolved
  case class SetProblemSize(size:Int)
  case class Progress(solved:Int, of:Int)

}

class Finalizer(monitor:ActorRef, proc:()=>Unit) extends Actor {
  monitor ! HasListeners.RegisterListener(self)

  protected def receive = {
    case ProgressMonitor.Progress(solved,of) if (solved == of) =>
      monitor ! HasListeners.DeregisterListener(self)
      proc()
      self.stop()
    case _ =>

  }
}
