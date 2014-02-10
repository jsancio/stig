
package object stig {
  type Decider = (DeciderContext, String) => Unit

  type Worker = (WorkerContext, String) => String

  final class ActivityFailedException(val reason: String, val cause: String) extends Exception
}
