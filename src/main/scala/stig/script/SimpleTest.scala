package stig.script

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient

import stig.{ DecisionManager, DeciderRegistration, DeciderContext, Activity }
import stig.model.Workflow

object SimpleTest extends App {
  val client = new AmazonSimpleWorkflowClient()
  client.setEndpoint("swf.us-west-1.amazonaws.com")

  val decisionManager = new DecisionManager(
    "test-domain",
    "test_task",
    client,
    Seq(DeciderRegistration(Workflow("simple-workflow", "1.1"), decider)))

  decisionManager.start()
  println("Press any key to shutdown...")
  readLine()
  decisionManager.stop()

  def decider(context: DeciderContext, input: String): Unit = {
    for {
      result <- context.scheduleActivity(
        Activity("simple-activity", "1.0"),
        "test_list",
        "cool input")
    } {
      context.completeWorkflow(s"decider's result: $result")
    }
  }
}
