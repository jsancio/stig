package stig

import stig.model.{ Activity, Workflow }
import stig.util.Later

trait DeciderContext {
  def startTimer(timeout: Int): Later[Unit]
  def scheduleActivity(activity: Activity, taskList: String, input: String): Later[String]
  def completeWorkflow(result: String)
  def failWorkflow(reason: String, details: String)
  def startChildWorkflow(workflow: Workflow, input: String): Unit
  def continueAsNewWorkflow(input: String): Unit
}
