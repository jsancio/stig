package stig.util

import scala.util.{ Try, Success, Failure }

trait Signal[T] {
  def later: Later[T]

  def isComplete: Boolean

  def complete(result: Try[T]): this.type

  def failure(t: Throwable): this.type = complete(Failure(t))

  def success(v: T): this.type = complete(Success(v))
}

object Signal {
  def apply[T](): Signal[T] = {
    new SignalImpl[T]()
  }

  final class SignalImpl[T] extends Signal[T] with Later[T] {
    private[this] var valueOption: Option[Try[T]] = None
    private[this] var functions = Seq[Try[T] => Any]()

    override def later: Later[T] = this

    override def isComplete: Boolean = valueOption.isDefined

    override def complete(result: Try[T]): SignalImpl.this.type = {
      require(!isComplete)

      valueOption = Some(result)

      functions foreach { func => func(result) }

      this
    }

    override def onComplete[U](func: Try[T] => U) {
      valueOption match {
        case Some(value) => func(value)
        case None => functions = functions :+ func
      }
    }
  }
}
