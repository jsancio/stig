package com.reverb.workflow.script

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.Executors

import collection.JavaConverters.asScalaBufferConverter
import concurrent.{ Future, ExecutionContext }

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient
import com.amazonaws.services.simpleworkflow.model.ActivityType
import com.amazonaws.services.simpleworkflow.model.CompleteWorkflowExecutionDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.Decision
import com.amazonaws.services.simpleworkflow.model.DecisionType
import com.amazonaws.services.simpleworkflow.model.PollForActivityTaskRequest
import com.amazonaws.services.simpleworkflow.model.PollForDecisionTaskRequest
import com.amazonaws.services.simpleworkflow.model.RespondActivityTaskCompletedRequest
import com.amazonaws.services.simpleworkflow.model.RespondDecisionTaskCompletedRequest
import com.amazonaws.services.simpleworkflow.model.ScheduleActivityTaskDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.StartWorkflowExecutionRequest
import com.amazonaws.services.simpleworkflow.model.TaskList
import com.amazonaws.services.simpleworkflow.model.WorkflowType

object LoadTest extends App {
  val continue = new AtomicBoolean(true)

  val domain = "test-domain"
  val taskList = new TaskList().withName("test_task")
  val client = new AmazonSimpleWorkflowClient()
  client.setEndpoint("swf.us-west-1.amazonaws.com")

  implicit val executor = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  for (index <- 1 to 10) {
    makeDecisions(index)
    makeActions(index)
  }

  println("Press any key to submit tasks...")
  readLine()

  println(s"Number of cores: ${Runtime.getRuntime().availableProcessors()}")
  val runs = for (index <- 1 to 200) yield {
    val run = client.startWorkflowExecution(new StartWorkflowExecutionRequest()
      .withDomain(domain)
      .withInput(s"$index")
      .withTaskList(taskList)
      .withWorkflowId(s"work-$index")
      .withTaskStartToCloseTimeout("360")
      .withWorkflowType(new WorkflowType()
        .withName("simple-workflow")
        .withVersion("1.1")))

    run.getRunId
  }
  println(s"Executed: ${runs.length} runs")

  println("Press any key to stop...")
  readLine()
  continue.set(false)
  println("Shutdown...")

  def makeActions(index: Int)(implicit executor: ExecutionContext): Unit = {
    Future {
      while (continue.get) {
        val request = new PollForActivityTaskRequest()
          .withDomain(domain)
          .withIdentity(s"activity-$index")
          .withTaskList(taskList)
        val activityTask = client.pollForActivityTask(request)

        for (taskToken <- Option(activityTask.getTaskToken)) {
          client.respondActivityTaskCompleted(new RespondActivityTaskCompletedRequest()
            .withResult("Some cool output")
            .withTaskToken(taskToken))
        }
      }
    }
  }

  def makeDecisions(index: Int)(implicit executor: ExecutionContext): Unit = {
    Future {
      while (continue.get) {
        val request = new PollForDecisionTaskRequest()
          .withDomain(domain)
          .withIdentity(s"decider-$index")
          .withTaskList(taskList)
        val decisionTask = client.pollForDecisionTask(request)

        for (taskToken <- Option(decisionTask.getTaskToken)) {
          val events = decisionTask.getEvents().asScala

          val endedCount = events.filter(event => activityEnded(event.getEventType)).length

          val response = if (endedCount >= 4) {
            // For now lets just complete the workflow
            new RespondDecisionTaskCompletedRequest()
              .withTaskToken(taskToken)
              .withDecisions(new Decision()
                .withDecisionType(DecisionType.CompleteWorkflowExecution)
                .withCompleteWorkflowExecutionDecisionAttributes(
                  new CompleteWorkflowExecutionDecisionAttributes().withResult("true")))
          } else {
            new RespondDecisionTaskCompletedRequest()
              .withTaskToken(taskToken)
              .withDecisions(new Decision()
                .withDecisionType(DecisionType.ScheduleActivityTask)
                .withScheduleActivityTaskDecisionAttributes(
                  new ScheduleActivityTaskDecisionAttributes()
                    .withActivityId("test_id")
                    .withScheduleToStartTimeout("360")
                    .withScheduleToCloseTimeout("360")
                    .withStartToCloseTimeout("360")
                    .withHeartbeatTimeout("360")
                    .withActivityType(new ActivityType()
                      .withName("simple-activity")
                      .withVersion("1.0"))
                    .withInput("Some cool input")
                    .withTaskList(taskList)))
          }

          client.respondDecisionTaskCompleted(response)
        }
      }
    }

    def activityEnded(eventType: String): Boolean = {
      eventType == "ActivityTaskCompleted" || eventType == "ActivityTaskFailed" ||
        eventType == "ActivityTaskTimedOut" || eventType == "ActivityTaskCanceled"
    }
  }
}
