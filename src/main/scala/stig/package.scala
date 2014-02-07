import scala.collection.JavaConverters._
import scala.concurrent.{ Future, ExecutionContext }

import java.util.UUID

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow
import com.amazonaws.services.simpleworkflow.model._
import org.joda.time.DateTime

import stig.util.Later
import stig.model.Workflow

package object stig {
  final class ExternalWorkflowClient(domain: String, client: AmazonSimpleWorkflow) {
    def startWorkflow(
      workflow: Workflow,
      input: String,
      tags: Seq[String]): String = {
      client.startWorkflowExecution(new StartWorkflowExecutionRequest()
        .withDomain(domain)
        .withExecutionStartToCloseTimeout("3600")
        .withInput(input)
        .withWorkflowId(UUID.randomUUID.toString) // TODO: we may want to change this
        .withTagList(tags.asJava)
        .withWorkflowType(new WorkflowType()
          .withName(workflow.name)
          .withVersion(workflow.version))).getRunId
    }

    def listWorkflow(tag: String): Seq[WorkflowExecutionInfo] = {
      val date = DateTime.now.minusDays(5)

      val opened = client.listOpenWorkflowExecutions(
        new ListOpenWorkflowExecutionsRequest()
          .withDomain(domain)
          .withStartTimeFilter(new ExecutionTimeFilter().withOldestDate(date.toDate))
          .withTagFilter(new TagFilter().withTag(tag)))

      val closed = client.listClosedWorkflowExecutions(
        new ListClosedWorkflowExecutionsRequest()
          .withDomain(domain)
          .withStartTimeFilter(new ExecutionTimeFilter().withOldestDate(date.toDate))
          .withTagFilter(new TagFilter().withTag(tag)))

      opened.getExecutionInfos.asScala ++ closed.getExecutionInfos.asScala
    }
  }
}
