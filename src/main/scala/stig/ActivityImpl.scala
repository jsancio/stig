package stig

import model.Activity

final class ActivityImpl extends WorkerContext {
  def executeActivity(
    activity: Activity,
    input: String,
    workers: Map[Activity, Worker]): String = {

    workers(activity)(this, input)
  }
}
