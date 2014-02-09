package stig

import java.util.concurrent.atomic.{ AtomicBoolean, AtomicReference }
import java.lang.management.ManagementFactory
import java.util.concurrent.Executors

import collection.JavaConverters._
import concurrent.{ Future, ExecutionContext, ExecutionContextExecutorService }

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow
import com.amazonaws.services.simpleworkflow.model._
import grizzled.slf4j.Logging

import stig.model.{ Decision, Workflow, DecisionConverter }

final class DecisionManager(
    domain: String,
    taskList: String,
    client: AmazonSimpleWorkflow,
    deciders: Map[Workflow, Decider]) extends Logging {

  @volatile private[this] var shutdown = true
  @volatile private[this] implicit var executor: ExecutionContextExecutorService = null

  def stop(): Unit = synchronized {
    require(!shutdown)
    shutdown = true
    executor.shutdown()
  }

  def start(): Unit = synchronized {
    require(shutdown)
    shutdown = false
    executor = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

    Future {
      run()
    }
  }

  private def run(): Unit = {
    try {
      val name = {
        val vmName = ManagementFactory.getRuntimeMXBean.getName
        val threadId = Thread.currentThread.getName

        s"$threadId:$vmName"
      }

      while (!shutdown) {
        info("Waiting on a decision task")
        val decisionTask = pollForDecisionTask(name)

        for (taskToken <- Option(decisionTask.getTaskToken)) {
          val runId = decisionTask.getWorkflowExecution.getRunId
          info(s"Processing decision task: $runId")

          val decisions = WorkflowEventsProcessor.makeDecisions(
            decisionTask.getPreviousStartedEventId,
            decisionTask.getStartedEventId,
            decisionTask.getEvents.asScala.map(WorkflowEventConverter.convert).flatten,
            deciders)

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

  private def pollForDecisionTask(name: String): DecisionTask = {
    client.pollForDecisionTask(
      new PollForDecisionTaskRequest()
        .withDomain(domain)
        .withTaskList(new TaskList().withName(taskList))
        .withIdentity(name))
  }

  // TODO: move this
  private def completeDecisionTask(taskToken: String, decisions: Iterable[Decision]): Unit = {
    val swfDecision = new RespondDecisionTaskCompletedRequest()
      .withTaskToken(taskToken)

    if (decisions.nonEmpty) {
      swfDecision.setDecisions(
        decisions.map(DecisionConverter.convert).asJavaCollection)
    }

    client.respondDecisionTaskCompleted(swfDecision)
  }
}
