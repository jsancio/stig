
package object stig {
  type Decider = (DeciderContext, String) => Unit

  type Worker = (WorkerContext, String) => String
}
