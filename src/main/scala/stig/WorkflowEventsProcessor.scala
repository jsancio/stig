package stig

import stig.model.{ WorkflowEvent, Workflow, Decision }

trait WorkflowEventsProcessor {
  def makeDecisions(
    previousId: Long,
    newId: Long,
    events: Iterable[WorkflowEvent],
    deciders: Map[Workflow, Decider]): Iterable[Decision]
}
