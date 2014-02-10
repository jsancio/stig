package stig

import model.{ WorkflowEvent, Workflow, Decision, Activity }

object Stig {
  def makeDecisions(
    previousId: Long,
    newId: Long,
    events: Iterable[WorkflowEvent],
    deciders: Map[Workflow, Decider]): Iterable[Decision] = {

    new DecisionImpl(previousId, newId).makeDecisions(events, deciders)
  }

  def executeActivity(
    activity: Activity,
    input: String,
    workders: Map[Activity, Worker]): String = {

    new ActivityImpl().executeActivity(activity, input, workders)
  }
}
