package stig

import stig.model.{ WorkflowEvent, Workflow, Decision }

object WorkflowEventsProcessor {
  def makeDecisions(
    previousId: Long,
    newId: Long,
    events: Iterable[WorkflowEvent],
    deciders: Map[Workflow, Decider]): Iterable[Decision] = {

    new DecisionImpl(previousId, newId).makeDecisions(events, deciders)
  }
}
