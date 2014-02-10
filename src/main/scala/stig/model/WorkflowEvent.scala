package stig.model

import org.joda.time.DateTime

sealed trait WorkflowEvent {
  val id: Long
  val timestamp: DateTime
}

object WorkflowEvent {
  final case class WorkflowExecutionStarted(
    id: Long,
    timestamp: DateTime,
    workflow: Workflow,
    input: String) extends WorkflowEvent

  final case class ActivityTaskScheduled(
    id: Long,
    timestamp: DateTime,
    activityId: Int) extends WorkflowEvent

  final case class ActivityTaskCompleted(
    id: Long,
    timestamp: DateTime,
    scheduledEventId: Long,
    result: String) extends WorkflowEvent

  final case class ActivityTaskFailed(
    id: Long,
    timestamp: DateTime,
    scheduledEventId: Long,
    reason: String,
    details: String) extends WorkflowEvent

  final case class TimerFired(
    id: Long,
    timestamp: DateTime,
    timerId: String) extends WorkflowEvent
}
