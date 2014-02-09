package stig.aws

import com.amazonaws.services.simpleworkflow.model.ActivityType
import com.amazonaws.services.simpleworkflow.model.CompleteWorkflowExecutionDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.ContinueAsNewWorkflowExecutionDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.DecisionType
import com.amazonaws.services.simpleworkflow.model.FailWorkflowExecutionDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.ScheduleActivityTaskDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.StartChildWorkflowExecutionDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.StartTimerDecisionAttributes
import com.amazonaws.services.simpleworkflow.model.TaskList
import com.amazonaws.services.simpleworkflow.model.WorkflowType
import com.amazonaws.services.simpleworkflow.model.{ Decision => SWFDecision }

import stig.model.Decision

object DecisionConverter {
  def convert(decision: Decision): SWFDecision = decision match {
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
            .withActivityType(
              new ActivityType()
                .withName(activity.name)
                .withVersion(activity.version))
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
