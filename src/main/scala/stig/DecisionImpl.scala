package stig

import java.util.UUID

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

import grizzled.slf4j.Logging

import stig.model.Activity
import stig.model.Workflow
import stig.model.Decision
import stig.model.WorkflowEvent
import stig.util.{ Later, Signal }

final class DecisionImpl(previousId: Long, newId: Long)
    extends InternalDeciderContext with Logging {

  private[this] var currentId = 0L
  private[this] var actionId = 0

  val decisions = mutable.Buffer.empty[Decision]
  val promises = mutable.Map.empty[Int, Signal[_]]

  private def removeSignal(id: Int): Signal[_] = {
    promises.remove(id).get
  }

  def isLive: Boolean = (currentId > previousId && currentId < newId)

  def nextId(): Int = {
    actionId += 1
    actionId
  }

  // TODO: communicate error
  def makeDecisions(
    events: Iterable[WorkflowEvent],
    deciders: Map[Workflow, Decider]): Iterable[Decision] = {

    val state = createDecisionState(events)
    var failed = false

    for (genericEvent <- events) {
      currentId = genericEvent.id

      try {
        genericEvent match {
          case event: WorkflowEvent.WorkflowExecutionStarted =>
            handleWorkflowExecutionStarted(event, deciders)

          case event: WorkflowEvent.ActivityTaskCompleted =>
            handleActivityTaskCompleted(event, state)

          case event: WorkflowEvent.TimerFired =>
            handleTimerFired(event)

          case _ =>
            debug(s"Skipping event: (${genericEvent.getClass.getName}, ${genericEvent.id})")
        }
      } catch {
        case NonFatal(failure) =>
          error("Unhandle decider exception", failure)
          failed = true
      }
    }

    if (!failed) {
      info(s"Made the following decisions: $decisions")
      decisions
    } else {
      Seq.empty[Decision]
    }
  }

  private[this] def createDecisionState(
    events: Iterable[WorkflowEvent]): Map[Long, WorkflowEvent] = {
    events.foldLeft(Map[Long, WorkflowEvent]()) {
      (state, event) => state + (event.id -> event)
    }
  }

  private[this] def handleWorkflowExecutionStarted(
    event: WorkflowEvent.WorkflowExecutionStarted,
    deciders: Map[Workflow, Decider]): Unit = {
    info(s"Handling workflow started event: ${event.id}")

    // Find the decider responsible for the start event
    deciders(event.workflow)(this, event.input)
  }

  private[this] def handleActivityTaskCompleted(
    event: WorkflowEvent.ActivityTaskCompleted,
    state: Map[Long, WorkflowEvent]): Unit = {

    info(s"Handling activity completed event: ${event.id}")

    // Handle activity task completed event
    val task = state(event.scheduledEventId).asInstanceOf[WorkflowEvent.ActivityTaskScheduled]

    val id = task.activityId.toInt
    val signal = removeSignal(id)

    // Complete the later for the Schedule task
    signal.asInstanceOf[Signal[String]] success event.result

    // TODO: handle the failure case
  }

  private[this] def handleTimerFired(event: WorkflowEvent.TimerFired): Unit = {
    info(s"Handling timer fired event: ${event.id}")

    val signal = removeSignal(event.timerId.toInt)

    // Complete the later for the Timer task
    signal.asInstanceOf[Signal[Unit]].success()
  }
}

trait InternalDeciderContext extends DeciderContext {
  val promises: mutable.Map[Int, Signal[_]]
  val decisions: mutable.Buffer[Decision]

  def isLive: Boolean
  def nextId(): Int

  def startTimer(timeout: Int): Later[Unit] = {
    val signal = Signal[Unit]()

    // Generate a predictable id
    val id = nextId()

    // If this is a replay then don't remember the decision
    if (isLive) {
      decisions += Decision.StartTimer(id, timeout.milliseconds)
    }

    // Remember the promise
    promises += (id -> signal)

    signal.later
  }

  def scheduleActivity(activity: Activity, taskList: String, input: String): Later[String] = {

    val signal = Signal[String]()

    // Generate a predictable id
    val id = nextId()

    // If this is a replay then don't remember the decision
    if (isLive) {
      decisions += Decision.ScheduleActivityTask(activity, taskList, id, input)
    }

    // Remember the promise
    promises += (id -> signal)

    signal.later
  }

  def completeWorkflow(result: String): Unit = {
    if (isLive) {
      decisions += Decision.CompleteWorkflow(result)
    }
  }

  def failWorkflow(reason: String, details: String): Unit = {
    if (isLive) {
      decisions += Decision.FailWorkflow(reason, details)
    }
  }

  def startChildWorkflow(workflow: Workflow, input: String): Unit = {
    if (isLive) {
      decisions += Decision.StartChildWorkflow(UUID.randomUUID.toString, workflow, input)
    }
  }

  def continueAsNewWorkflow(input: String): Unit = {
    if (isLive) {
      decisions += Decision.ContinueAsNewWorkflow(input)
    }
  }
}
