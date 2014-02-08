package stig

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.net.InetAddress

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ future, Future, ExecutionContext }
import scala.util.control.NonFatal

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow
import com.amazonaws.services.simpleworkflow.model._
import grizzled.slf4j.Logging

import stig.model.Activity

final class ActivityManager(
    domain: String,
    taskList: String,
    client: AmazonSimpleWorkflow,
    registrations: Seq[ActivityRegistration]) extends Logging {
  private[this] val started = new AtomicBoolean(false)
  private[this] val shutdown = new AtomicBoolean(false)

  def stop() {
    require(started.get)
    shutdown.set(true)
  }

  def start() {
    require(!started.get)
    started.set(true)

    val workers = registrations.foldLeft(Map[Activity, ActivityManager.Worker]()) {
      (state, current) => state + (current.activity -> current.worker)
    }

    new Thread(new ActivityRunnable(workers)).start()
  }

  private[this] final class ActivityRunnable(
      workers: Map[Activity, ActivityManager.Worker]) extends Runnable {
    def run() {
      try {
        val name = {
          val hostname = InetAddress.getLocalHost.getHostName
          val threadId = Thread.currentThread.getName

          s"$hostname:$threadId"
        }

        while (!shutdown.get) {
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
  }

  private[this] def convert(activityType: ActivityType): Activity = {
    Activity(activityType.getName, activityType.getVersion)
  }

  private[this] def pollForActivityTask(
    name: String,
    domain: String,
    taskList: String): ActivityTask = {
    client.pollForActivityTask(
      new PollForActivityTaskRequest()
        .withDomain(domain)
        .withTaskList(new TaskList().withName(taskList))
        .withIdentity(name))
  }

  private[this] def completeActivityTask(taskToken: String, result: String) {
    client.respondActivityTaskCompleted(
      new RespondActivityTaskCompletedRequest()
        .withTaskToken(taskToken)
        .withResult(result))
  }

  private[this] def failActivityTask(
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

object ActivityManager {
  type Worker = (WorkerContext, String) => String
}

private final class WorkerContextImpl extends WorkerContext

trait WorkerContext

case class ActivityRegistration(activity: Activity, worker: ActivityManager.Worker)
