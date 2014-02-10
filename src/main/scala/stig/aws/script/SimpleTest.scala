package stig.aws.script

import scala.util.{ Success, Failure }

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient

import stig.aws.{ DecisionManager, ActivityManager }
import stig.model.{ Workflow, Activity }
import stig.{ DeciderContext, WorkerContext }

object SimpleTest extends App {
  object Activities {
    val simpleActivity = Activity("simple-activity", "1.0")
    val faultyActivity = Activity("faulty-activity", "1.0")
  }

  val client = new AmazonSimpleWorkflowClient()
  client.setEndpoint("swf.us-west-1.amazonaws.com")

  val domain = "test-domain"
  val taskList = "test_task"

  val decisionManager = new DecisionManager(
    domain,
    taskList,
    client,
    Map(Workflow("simple-workflow", "1.1") -> decider))

  val activityManager = new ActivityManager(
    domain,
    taskList,
    client,
    Map(Activities.simpleActivity -> activity, Activities.faultyActivity -> faultyActivity))

  decisionManager.start()
  activityManager.start()
  println("Press any key to shutdown...")
  readLine()
  activityManager.stop()
  decisionManager.stop()

  def decider(context: DeciderContext, input: String): Unit = {
    // TODO: implement failure recovery
    context.scheduleActivity(Activities.faultyActivity, taskList, input).onComplete {
      case Success(_) =>
        // this should never happend. fail the workflow
        context.failWorkflow("Unexpected behavior", "Faulty activity should never succeed")

      case Failure(e) =>
        // TODO: what should we do with e?
        context.scheduleActivity(Activities.simpleActivity, taskList, input).foreach { result =>
          context.completeWorkflow(s"decider's result: $result")
        }
    }
  }

  def activity(context: WorkerContext, input: String): String = {
    s"activity result: $input"
  }

  def faultyActivity(context: WorkerContext, input: String): String = {
    throw new IllegalStateException("This activity always fails")
  }
}
