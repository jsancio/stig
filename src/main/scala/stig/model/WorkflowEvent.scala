package stig.model

import org.joda.time.DateTime

sealed trait WorkflowEvent {
  val id: Long
  val timestamp: DateTime
}

final case class WorkflowExecutionStarted(
  id: Long,
  timestamp: DateTime,
  workflow: Workflow,
  input: String) extends WorkflowEvent

final case class ActivityTaskScheduled(
  id: Long,
  timestamp: DateTime,
  activityId: String) extends WorkflowEvent

final case class ActivityTaskCompleted(
  id: Long,
  timestamp: DateTime,
  scheduledEventId: Long,
  result: String) extends WorkflowEvent

final case class TimerFired(
  id: Long,
  timestamp: DateTime,
  timerId: String) extends WorkflowEvent

// TODO: move this out of there

case class Workflow(name: String, version: String)