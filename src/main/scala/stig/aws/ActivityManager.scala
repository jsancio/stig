package stig.aws

import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.concurrent.{ Future, ExecutionContext, ExecutionContextExecutorService }
import scala.util.control.NonFatal

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow
import com.amazonaws.services.simpleworkflow.model._
import grizzled.slf4j.Logging

import stig.model.Activity
import stig.{ Worker, WorkerContextImpl }
import util.actorName

final class ActivityManager(
    domain: String,
    taskList: String,
    client: AmazonSimpleWorkflow,
    workers: Map[Activity, Worker]) extends Logging {

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

  private def run() {
    try {
      val name = actorName

      while (!shutdown) {
        info("Waiting on an activity task")
        val activityTask = pollForActivityTask(name, domain, taskList)

        for (taskToken <- Option(activityTask.getTaskToken)) {
          info(s"Processing activity ${activityTask.getActivityId}")

          val context = new WorkerContextImpl()
          val resultOption = try {
            val result = workers(convert(activityTask.getActivityType))(
              context,
              activityTask.getInput)

            info(s"Finished activity ${activityTask.getActivityId}")

            Some(result)
          } catch {
            case NonFatal(e) => {
              error("Unhandle activity exception", e)
              // TODO: report better errors
              failActivityTask(taskToken, "Failed!", "Detail fail description")
              None
            }
          }

          for (result <- resultOption) {
            completeActivityTask(taskToken, result)
          }
        }
      }
    } catch {
      case e: Throwable => {
        error("Unhandle exception:", e)
        throw e
      }
    }
  }

  private def convert(activityType: ActivityType): Activity = {
    Activity(activityType.getName, activityType.getVersion)
  }

  private def pollForActivityTask(
    name: String,
    domain: String,
    taskList: String): ActivityTask = {
    client.pollForActivityTask(
      new PollForActivityTaskRequest()
        .withDomain(domain)
        .withTaskList(new TaskList().withName(taskList))
        .withIdentity(name))
  }

  private def completeActivityTask(taskToken: String, result: String) {
    client.respondActivityTaskCompleted(
      new RespondActivityTaskCompletedRequest()
        .withTaskToken(taskToken)
        .withResult(result))
  }

  private def failActivityTask(
    taskToken: String,
    reason: String,
    detail: String) {
    client.respondActivityTaskFailed(
      new RespondActivityTaskFailedRequest()
        .withTaskToken(taskToken)
        .withReason(reason)
        .withDetails(detail))
  }
}
