package stig.aws

import java.lang.management.ManagementFactory

package object util {
  def actorName(): String = {
    val vmName = ManagementFactory.getRuntimeMXBean.getName
    val threadId = Thread.currentThread.getName

    s"$threadId@$vmName"
  }
}
