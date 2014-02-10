package stig.aws

import org.joda.time.DateTime

import com.amazonaws.services.simpleworkflow.model.ActivityType
import com.amazonaws.services.simpleworkflow.model.CompleteWorkflowExecutionDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.ContinueAsNewWorkflowExecutionDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.DecisionType
import com.amazonaws.services.simpleworkflow.model.FailWorkflowExecutionDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.HistoryEvent
import com.amazonaws.services.simpleworkflow.model.ScheduleActivityTaskDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.StartChildWorkflowExecutionDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.StartTimerDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.TaskList
import com.amazonaws.services.simpleworkflow.model.WorkflowType
import com.amazonaws.services.simpleworkflow.model.WorkflowType
import com.amazonaws.services.simpleworkflow.model.{ Decision => SWFDecision }

import stig.model.{ Activity, Decision, WorkflowEvent, Workflow }

object StigConverter {
  implicit class ActivityTypeConverter(val activityType: ActivityType) extends AnyVal {
    def asStig: Activity = {
      Activity(activityType.getName, activityType.getVersion)
    }
  }

  implicit class ActivityConverter(val activity: Activity) extends AnyVal {
    def asAws: ActivityType = {
      new ActivityType()
        .withName(activity.name)
        .withVersion(activity.version)
    }
  }

  implicit class DecisionConverter(val decision: Decision) extends AnyVal {
    def asAws: SWFDecision = decision match {
      case Decision.ContinueAsNewWorkflow(input) =>
        new SWFDecision()
          .withDecisionType(DecisionType.ContinueAsNewWorkflowExecution)
          .withContinueAsNewWorkflowExecutionDecisionAttributes(
            new ContinueAsNewWorkflowExecutionDecisionAttributes()
              .withInput(input))

      case Decision.StartChildWorkflow(id, workflow, input) =>
        new SWFDecision()
          .withDecisionType(DecisionType.StartChildWorkflowExecution)
          .withStartChildWorkflowExecutionDecisionAttributes(
            new StartChildWorkflowExecutionDecisionAttributes()
              .withInput(input)
              .withWorkflowId(id)
              .withWorkflowType(new WorkflowType()
                .withName(workflow.name)
                .withVersion(workflow.version)))

      case Decision.FailWorkflow(reason, details) =>
        new SWFDecision()
          .withDecisionType(DecisionType.FailWorkflowExecution)
          .withFailWorkflowExecutionDecisionAttributes(
            new FailWorkflowExecutionDecisionAttributes()
              .withDetails(details)
              .withReason(reason))

      case Decision.CompleteWorkflow(result) =>
        new SWFDecision()
          .withDecisionType(DecisionType.CompleteWorkflowExecution)
          .withCompleteWorkflowExecutionDecisionAttributes(
            new CompleteWorkflowExecutionDecisionAttributes()
              .withResult(result))

      case Decision.ScheduleActivityTask(activity, taskList, id, input) =>
        new SWFDecision()
          .withDecisionType(DecisionType.ScheduleActivityTask)
          .withScheduleActivityTaskDecisionAttributes(
            new ScheduleActivityTaskDecisionAttributes()
              .withActivityId(id.toString)
              .withActivityType(activity.asAws)
              .withInput(input)
              .withStartToCloseTimeout("NONE")
              .withScheduleToStartTimeout("NONE")
              .withScheduleToCloseTimeout("NONE")
              .withHeartbeatTimeout("NONE")
              .withTaskList(new TaskList().withName(taskList)))

      case Decision.StartTimer(id, timeout) =>
        new SWFDecision()
          .withDecisionType(DecisionType.StartTimer)
          .withStartTimerDecisionAttributes(
            new StartTimerDecisionAttributes()
              .withTimerId(id.toString)
              .withStartToFireTimeout(timeout.toString))
    }
  }

  implicit class HistoryEventConverter(val event: HistoryEvent) extends AnyVal {
    def asStig: Option[WorkflowEvent] = event.getEventType match {
      case "WorkflowExecutionStarted" =>
        val attributes = event.getWorkflowExecutionStartedEventAttributes
        Some(WorkflowEvent.WorkflowExecutionStarted(
          event.getEventId,
          new DateTime(event.getEventTimestamp),
          attributes.getWorkflowType.asStig,
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

  implicit class WorkflowTypeConverter(val workflow: WorkflowType) extends AnyVal {
    def asStig: Workflow = {
      Workflow(workflow.getName, workflow.getVersion)
    }
  }

  implicit class WorkflowConverter(val workflow: Workflow) extends AnyVal {
    def asAws: WorkflowType = {
      new WorkflowType()
        .withName(workflow.name)
        .withVersion(workflow.version)
    }
  }
}
