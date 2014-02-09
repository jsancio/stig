package stig.model

import concurrent.duration.Duration

sealed trait Decision

object Decision {
  final case class ContinueAsNewWorkflow(input: String) extends Decision
  final case class StartChildWorkflow(id: String, workflow: Workflow, input: String)
    extends Decision
  final case class FailWorkflow(reason: String, details: String) extends Decision
  final case class CompleteWorkflow(result: String) extends Decision
  final case class ScheduleActivityTask(
    activity: Activity,
    taskList: String,
    id: Int,
    input: String) extends Decision
  final case class StartTimer(id: Int, timeout: Duration) extends Decision
}
