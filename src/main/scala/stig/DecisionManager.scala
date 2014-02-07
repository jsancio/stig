package stig

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicBoolean
import java.util.UUID

import collection.JavaConverters._
import collection.mutable
import scala.util.control.NonFatal
import scala.util.Try

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow
import com.amazonaws.services.simpleworkflow.model._
import grizzled.slf4j.Logging

import stig.model.ActivityTaskCompleted
import stig.model.ActivityTaskScheduled
import stig.model.TimerFired
import stig.model.Workflow
import stig.model.WorkflowEvent
import stig.model.WorkflowExecutionStarted
import stig.util.{ Signal, Later }

final class DecisionManager(
    domain: String,
    taskList: String,
    client: AmazonSimpleWorkflow,
    registrations: Seq[DeciderRegistration]) extends Logging {
  private[this] val started = new AtomicBoolean(false)
  private[this] val shutdown = new AtomicBoolean(false)

  def stop() {
    require(started.get)
    shutdown.set(true)
  }

  def start() {
    require(!started.get)
    started.set(true)

    val deciders = registrations.foldLeft(Map[Workflow, DecisionManager.Decider]()) {
      (state, current) => state + (current.workflow -> current.decider)
    }

    new Thread(new DecisionRunnable(deciders)).start()
  }

  private[this] final class DecisionRunnable(
      deciders: Map[Workflow, DecisionManager.Decider]) extends Runnable {
    def run() {
      try {
        val name = {
          val hostname = InetAddress.getLocalHost.getHostName
          val threadId = Thread.currentThread.getName

          s"$hostname:$threadId"
        }

        while (!shutdown.get) {
          info("Waiting on a decision task")
          val decisionTask = pollForDecisionTask(name, domain, taskList)

          for (taskToken <- Option(decisionTask.getTaskToken)) {
            val runId = decisionTask.getWorkflowExecution.getRunId
            info(s"Processing decision task: $runId")

            val events = decisionTask.getEvents.asScala.map(WorkflowEventConverter.convert).flatten
            val state = createDecisionState(events)
            val previousId = decisionTask.getPreviousStartedEventId
            val newId = decisionTask.getStartedEventId
            val context = new DecisionContextImpl(previousId, newId)

            val failures = (for (genericEvent <- events) yield {
              context.currentId = genericEvent.id

              genericEvent match {
                case event: WorkflowExecutionStarted =>
                  handleWorkflowExecutionStarted(event, deciders, context)

                case event: ActivityTaskCompleted =>
                  handleActivityTaskCompleted(event, state, context)

                case event: TimerFired =>
                  handleTimerFired(event, context)

                case _ =>
                  debug(s"Skipping event: (${genericEvent.getClass.getName}, ${genericEvent.id})")
                  None // do nothing
              }
            }).flatten

            if (failures.isEmpty) {
              // Send all the decisions
              info(s"Made the following decisions: ${context.decisions}")

              completeDecisionTask(taskToken, context.decisions)
            } else {
              for (failure <- failures) { error("Unhandle decider exception", failure) }
            }
          }
        }
      } catch {
        case e: Throwable => {
          error("Unhandle exception", e)
          throw e
        }
      }
    }
  }

  private[this] def handleWorkflowExecutionStarted(
    event: WorkflowExecutionStarted,
    deciders: Map[Workflow, DecisionManager.Decider],
    context: DecisionContextImpl): Option[Throwable] = {
    info(s"Handling workflow started event: ${event.id}")

    // Find the decider responsible for the start event
    try {
      deciders(event.workflow)(
        context,
        event.input)
      None
    } catch {
      case NonFatal(e) => Some(e)
    }
  }

  private[this] def handleActivityTaskCompleted(
    event: ActivityTaskCompleted,
    state: Map[Long, WorkflowEvent],
    context: DecisionContextImpl): Option[Throwable] = {
    info(s"Handling activity completed event: ${event.id}")

    // Handle activity task completed event
    val task = state(event.scheduledEventId).asInstanceOf[ActivityTaskScheduled]

    val id = task.activityId.toInt
    val signal = context.removeSignal(id).get

    // Execute the later!
    try {
      signal.asInstanceOf[Signal[String]] success event.result
      None
    } catch {
      case NonFatal(e) => Some(e)
    }
  }

  private[this] def handleTimerFired(
    event: TimerFired,
    context: DecisionContextImpl): Option[Throwable] = {
    info(s"Handling timer fired event: ${event.id}")

    val signal = context.removeSignal(event.timerId.toInt).get

    try {
      signal.asInstanceOf[Signal[Unit]].success()
      None
    } catch {
      case NonFatal(e) => Some(e)
    }
  }

  private[this] def convert(wtype: WorkflowType): Workflow = {
    Workflow(wtype.getName, wtype.getVersion)
  }

  private[this] def pollForDecisionTask(
    name: String,
    domain: String,
    taskList: String): DecisionTask = {

    client.pollForDecisionTask(
      new PollForDecisionTaskRequest()
        .withDomain(domain)
        .withTaskList(new TaskList().withName(taskList))
        .withIdentity(name))
  }

  private[this] def completeDecisionTask(
    taskToken: String,
    decisions: Seq[Decision]) {
    val swfDecision = new RespondDecisionTaskCompletedRequest()
      .withTaskToken(taskToken)

    if (decisions.nonEmpty) {
      swfDecision.setDecisions(decisions.asJava)
    }

    client.respondDecisionTaskCompleted(swfDecision)
  }

  private[this] def createDecisionState(
    events: mutable.Buffer[WorkflowEvent]): Map[Long, WorkflowEvent] = {
    events.foldLeft(Map[Long, WorkflowEvent]()) {
      (state, event) => state + (event.id -> event)
    }
  }
}

object DecisionManager {
  type Decider = (DeciderContext, String) => Unit
}

private final class DecisionContextImpl(
    previousId: Long,
    newId: Long) extends DeciderContext {
  private[this] var _decisions = Seq[Decision]()
  private[this] val _promises = mutable.Map[Int, Signal[_]]()

  def decisions: Seq[Decision] = _decisions

  def removeSignal(id: Int): Option[Signal[_]] = {
    _promises.remove(id)
  }

  var currentId = 0L

  override def startTimer(timeout: Int): Later[Unit] = {
    val signal = Signal[Unit]()

    // Generate a predictable id
    val id = idGenerator.nextId

    // If this is a replay then don't remember the decision
    if (isLive) {
      _decisions = _decisions :+ new Decision()
        .withDecisionType(DecisionType.StartTimer)
        .withStartTimerDecisionAttributes(
          new StartTimerDecisionAttributes()
            .withTimerId(id.toString)
            .withStartToFireTimeout(timeout.toString))
    }

    // Remember the promise
    _promises += (id -> signal)

    signal.later
  }

  override def scheduleActivity(
    activity: Activity,
    taskList: String,
    input: String): Later[String] = {

    val signal = Signal[String]()

    // Generate a predictable id
    val id = idGenerator.nextId

    // If this is a replay then don't remember the decision
    if (isLive) {
      _decisions = _decisions :+ new Decision()
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
    }

    // Remember the promise
    _promises += (id -> signal)

    signal.later
  }

  override def completeWorkflow(result: String) {
    if (isLive) {
      _decisions = _decisions :+ new Decision()
        .withDecisionType(DecisionType.CompleteWorkflowExecution)
        .withCompleteWorkflowExecutionDecisionAttributes(
          new CompleteWorkflowExecutionDecisionAttributes()
            .withResult(result))
    }
  }

  override def failWorkflow(reason: String, details: String) {
    if (isLive) {
      _decisions = _decisions :+ new Decision()
        .withDecisionType(DecisionType.FailWorkflowExecution)
        .withFailWorkflowExecutionDecisionAttributes(
          new FailWorkflowExecutionDecisionAttributes()
            .withDetails(details)
            .withReason(reason))
    }
  }

  override def startChildWorkflow(workflow: Workflow, input: String) {
    if (isLive) {
      _decisions = _decisions :+ new Decision()
        .withDecisionType(DecisionType.StartChildWorkflowExecution)
        .withStartChildWorkflowExecutionDecisionAttributes(
          new StartChildWorkflowExecutionDecisionAttributes()
            .withInput(input)
            .withWorkflowId(UUID.randomUUID.toString)
            .withWorkflowType(new WorkflowType()
              .withName(workflow.name)
              .withVersion(workflow.version)))
    }
  }

  override def continueAsNewWorkflow(input: String) {
    if (isLive) {
      _decisions = _decisions :+ new Decision()
        .withDecisionType(DecisionType.ContinueAsNewWorkflowExecution)
        .withContinueAsNewWorkflowExecutionDecisionAttributes(
          new ContinueAsNewWorkflowExecutionDecisionAttributes()
            .withInput(input))
    }
  }

  private[this] def isLive: Boolean = (currentId > previousId && currentId < newId)

  private final object idGenerator {
    private[this] var state = 0

    def nextId(): Int = {
      state = state + 1
      state
    }
  }
}

trait DeciderContext {
  def scheduleActivity(activity: Activity, taskList: String, input: String): Later[String]
  def completeWorkflow(result: String)
  def failWorkflow(details: String, reason: String)
  def startTimer(timeout: Int): Later[Unit]
  def startChildWorkflow(workflow: Workflow, input: String)
  def continueAsNewWorkflow(input: String)
}

case class DeciderRegistration(workflow: Workflow, decider: DecisionManager.Decider)
