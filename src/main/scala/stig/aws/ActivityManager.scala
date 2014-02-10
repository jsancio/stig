package stig.aws

import java.util.concurrent.Executors

import scala.concurrent.{ Future, ExecutionContext, ExecutionContextExecutorService }
import scala.util.control.NonFatal

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow
import com.amazonaws.services.simpleworkflow.model.ActivityTask
import com.amazonaws.services.simpleworkflow.model.PollForActivityTaskRequest
import com.amazonaws.services.simpleworkflow.model.RespondActivityTaskCompletedRequest
import com.amazonaws.services.simpleworkflow.model.RespondActivityTaskFailedRequest
import com.amazonaws.services.simpleworkflow.model.TaskList
import grizzled.slf4j.Logging

import stig.model.Activity
import stig.{ Worker, Stig }
import StigConverter.ActivityTypeConverter
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

          val resultOption = try {
            val result = Stig.executeActivity(
              activityTask.getActivityType.asStig,
              activityTask.getInput,
              workers)

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
