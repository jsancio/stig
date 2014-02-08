package stig

import java.util.UUID

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

import grizzled.slf4j.Logging

import stig.model.Activity
import stig.model.Workflow
import stig.model.Decision
import stig.model.ContinueAsNewWorkflow
import stig.model.StartChildWorkflow
import stig.model.CompleteWorkflow
import stig.model.FailWorkflow
import stig.model.ScheduleActivityTask
import stig.model.StartTimer
import stig.model.WorkflowEvent
import stig.model.ActivityTaskScheduled
import stig.model.TimerFired
import stig.model.ActivityTaskCompleted
import stig.model.WorkflowExecutionStarted
import stig.util.{ Later, Signal }

final class DecisionImpl()
    extends InternalDeciderContext
    with WorkflowEventsProcessor with Logging {

  private[this] var currentId = 0L
  private[this] var previousId = 0L
  private[this] var newId = 0L
  private[this] var state = 0

  val decisions = mutable.Buffer.empty[Decision]
  val promises = mutable.Map.empty[Int, Signal[_]]

  private def removeSignal(id: Int): Signal[_] = {
    promises.remove(id).get
  }

  def isLive: Boolean = (currentId > previousId && currentId < newId)

  def nextId(): Int = {
    state = state + 1
    state
  }

  // TODO: communicate error
  override def makeDecisions(
    previousId: Long,
    newId: Long,
    events: Iterable[WorkflowEvent],
    deciders: Map[Workflow, Decider]): Iterable[Decision] = {

    this.previousId = previousId
    this.newId = newId

    val state = createDecisionState(events)
    var failed = false

    for (genericEvent <- events) {
      currentId = genericEvent.id

      try {
        genericEvent match {
          case event: WorkflowExecutionStarted =>
            handleWorkflowExecutionStarted(event, deciders)

          case event: ActivityTaskCompleted =>
            handleActivityTaskCompleted(event, state)

          case event: TimerFired =>
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
    event: WorkflowExecutionStarted,
    deciders: Map[Workflow, Decider]): Option[Throwable] = {
    info(s"Handling workflow started event: ${event.id}")

    // Find the decider responsible for the start event
    try {
      deciders(event.workflow)(this, event.input)
      None
    } catch {
      case NonFatal(e) => Some(e)
    }
  }

  private[this] def handleActivityTaskCompleted(
    event: ActivityTaskCompleted,
    state: Map[Long, WorkflowEvent]): Option[Throwable] = {

    info(s"Handling activity completed event: ${event.id}")

    // Handle activity task completed event
    val task = state(event.scheduledEventId).asInstanceOf[ActivityTaskScheduled]

    val id = task.activityId.toInt
    val signal = removeSignal(id)

    // Execute the later!
    try {
      signal.asInstanceOf[Signal[String]] success event.result
      None
    } catch {
      case NonFatal(e) => Some(e)
    }
  }

  private[this] def handleTimerFired(event: TimerFired): Option[Throwable] = {
    info(s"Handling timer fired event: ${event.id}")

    val signal = removeSignal(event.timerId.toInt)

    try {
      signal.asInstanceOf[Signal[Unit]].success()
      None
    } catch {
      case NonFatal(e) => Some(e)
    }
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
      decisions += StartTimer(id, timeout.milliseconds)
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
      decisions += ScheduleActivityTask(activity, taskList, id, input)
    }

    // Remember the promise
    promises += (id -> signal)

    signal.later
  }

  def completeWorkflow(result: String) {
    if (isLive) {
      decisions += CompleteWorkflow(result)
    }
  }

  def failWorkflow(reason: String, details: String) {
    if (isLive) {
      decisions += FailWorkflow(reason, details)
    }
  }

  def startChildWorkflow(workflow: Workflow, input: String): Unit = {
    if (isLive) {
      decisions += StartChildWorkflow(UUID.randomUUID.toString, workflow, input)
    }
  }

  def continueAsNewWorkflow(input: String): Unit = {
    if (isLive) {
      decisions += ContinueAsNewWorkflow(input)
    }
  }
}

