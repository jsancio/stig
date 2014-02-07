package stig.util

import scala.util.{ Try, Success, Failure }
import scala.util.control.NonFatal

trait Later[T] {
  def isComplete: Boolean

  def onComplete[U](func: Try[T] => U): Unit

  def filter(pred: T => Boolean): Later[T] = {
    val signal = Signal[T]()

    onComplete {
      case f: Failure[_] => signal complete f.asInstanceOf[Failure[T]]
      case Success(v) =>
        try {
          if (pred(v)) signal success v
          else {
            signal failure (
              new NoSuchElementException("Later.filter predicate is not satisfied"))
          }
        } catch {
          case NonFatal(e) => signal failure e
        }
    }

    signal.later
  }

  def withFilter(p: T => Boolean): Later[T] = filter(p)

  def flatMap[S](f: T => Later[S]): Later[S] = {
    val signal = Signal[S]()

    onComplete {
      case f: Failure[_] => signal complete f.asInstanceOf[Failure[S]]
      case Success(v) =>
        try {
          f(v) onComplete {
            case f: Failure[_] => signal complete f.asInstanceOf[Failure[S]]
            case Success(v) => signal success v
          }
        } catch {
          case NonFatal(e) => signal failure e
        }
    }

    signal.later
  }

  def foreach[U](f: T => U) {
    onComplete {
      case Success(v) => f(v)
      case _ => // nothing to do on failure
    }
  }

  def map[S](f: T => S): Later[S] = {
    val signal = Signal[S]()

    onComplete {
      case f: Failure[_] => signal complete f.asInstanceOf[Failure[S]]
      case Success(v) =>
        try {
          signal success f(v)
        } catch {
          case NonFatal(e) => signal failure e
        }
    }

    signal.later
  }
}
