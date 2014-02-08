package stig

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicBoolean
import java.util.UUID

import concurrent.duration.DurationInt
import collection.JavaConverters._
import collection.mutable
import scala.util.Try

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow
import com.amazonaws.services.simpleworkflow.model._
import grizzled.slf4j.Logging

import stig.model.ActivityTaskCompleted
import stig.model.ActivityTaskScheduled
import stig.model.Activity
import stig.model.Decision
import stig.model.TimerFired
import stig.model.Workflow
import stig.model.WorkflowEvent
import stig.model.WorkflowExecutionStarted
import stig.model.ContinueAsNewWorkflow
import stig.model.CompleteWorkflow
import stig.model.StartChildWorkflow
import stig.model.FailWorkflow
import stig.model.ScheduleActivityTask
import stig.model.StartTimer
import stig.model.DecisionConverter
import stig.util.{ Signal, Later }

final class DecisionManager(
    domain: String,
    taskList: String,
    client: AmazonSimpleWorkflow,
    registrations: Seq[DeciderRegistration]) extends Logging {
  private[this] val started = new AtomicBoolean(false)
  private[this] val shutdown = new AtomicBoolean(false)

  def stop() {
    require(started.get)
    shutdown.set(true)
  }

  def start() {
    require(!started.get)
    started.set(true)

    val deciders = registrations.foldLeft(Map[Workflow, Decider]()) {
      (state, current) => state + (current.workflow -> current.decider)
    }

    new Thread(new DecisionRunnable(deciders)).start()
  }

  private[this] final class DecisionRunnable(
      deciders: Map[Workflow, Decider]) extends Runnable {
    def run() {
      try {
        val name = {
          val hostname = InetAddress.getLocalHost.getHostName
          val threadId = Thread.currentThread.getName

          s"$threadId:$hostname"
        }

        while (!shutdown.get) {
          info("Waiting on a decision task")
          val decisionTask = pollForDecisionTask(name, domain, taskList)

          for (taskToken <- Option(decisionTask.getTaskToken)) {
            val runId = decisionTask.getWorkflowExecution.getRunId
            info(s"Processing decision task: $runId")

            val events = decisionTask.getEvents.asScala.map(WorkflowEventConverter.convert).flatten
            val previousId = decisionTask.getPreviousStartedEventId
            val newId = decisionTask.getStartedEventId
            val context = new DecisionImpl()

            val decisions = context.makeDecisions(previousId, newId, events, deciders)

            // TODO: this needs to deal with errors
            completeDecisionTask(taskToken, decisions)
          }
        }
      } catch {
        case e: Throwable => {
          error("Unhandle exception", e)
          throw e
        }
      }
    }
  }

  private[this] def pollForDecisionTask(
    name: String,
    domain: String,
    taskList: String): DecisionTask = {

    client.pollForDecisionTask(
      new PollForDecisionTaskRequest()
        .withDomain(domain)
        .withTaskList(new TaskList().withName(taskList))
        .withIdentity(name))
  }

  // TODO: move this
  private[this] def completeDecisionTask(
    taskToken: String,
    decisions: Iterable[Decision]) {
    val swfDecision = new RespondDecisionTaskCompletedRequest()
      .withTaskToken(taskToken)

    if (decisions.nonEmpty) {
      swfDecision.setDecisions(
        decisions.map(DecisionConverter.convert).asJavaCollection)
    }

    client.respondDecisionTaskCompleted(swfDecision)
  }
}

case class DeciderRegistration(workflow: Workflow, decider: Decider)
