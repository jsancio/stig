package stig

import org.joda.time.DateTime
import com.amazonaws.services.simpleworkflow.model.HistoryEvent
import com.amazonaws.services.simpleworkflow.model.WorkflowType

import stig.model.{ WorkflowEvent, Workflow }

object WorkflowEventConverter {
  def convert(event: HistoryEvent): Option[WorkflowEvent] = {
    event.getEventType match {
      case "WorkflowExecutionStarted" =>
        val attributes = event.getWorkflowExecutionStartedEventAttributes
        Some(WorkflowEvent.WorkflowExecutionStarted(
          event.getEventId,
          new DateTime(event.getEventTimestamp),
          convert(attributes.getWorkflowType),
          attributes.getInput))

      case "ActivityTaskScheduled" =>
        val attributes = event.getActivityTaskScheduledEventAttributes
        Some(WorkflowEvent.ActivityTaskScheduled(
          event.getEventId,
          new DateTime(event.getEventTimestamp),
          attributes.getActivityId))

      case "ActivityTaskCompleted" =>
        val attributes = event.getActivityTaskCompletedEventAttributes
        Some(WorkflowEvent.ActivityTaskCompleted(
          event.getEventId,
          new DateTime(event.getEventTimestamp),
          attributes.getScheduledEventId,
          attributes.getResult))

      case "TimerFired" =>
        val attributes = event.getTimerFiredEventAttributes
        Some(WorkflowEvent.TimerFired(
          event.getEventId,
          new DateTime(event.getEventTimestamp),
          attributes.getTimerId))

      case _ => None
    }
  }

  def convert(workflow: WorkflowType): Workflow = {
    Workflow(workflow.getName, workflow.getVersion)
  }
}
