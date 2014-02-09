package stig.aws.script

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient

import stig.aws.{ DecisionManager, ActivityManager }
import stig.model.{ Workflow, Activity }
import stig.{ DeciderContext, WorkerContext }

object SimpleTest extends App {
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
    Map(Activity("simple-activity", "1.0") -> activity))

  decisionManager.start()
  activityManager.start()
  println("Press any key to shutdown...")
  readLine()
  activityManager.stop()
  decisionManager.stop()

  def decider(context: DeciderContext, input: String): Unit = {
    for {
      result <- context.scheduleActivity(Activity("simple-activity", "1.0"), taskList, input)
    } {
      context.completeWorkflow(s"decider's result: $result")
    }
  }

  def activity(context: WorkerContext, input: String): String = {
    s"activity result: $input"
  }
}
