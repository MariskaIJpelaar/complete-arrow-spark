package nl.liacs.mijpelaar.utils

import scala.util.{Failure, Success, Try}


object Resources {
  /** from: https://stackoverflow.com/a/39868021 */
  def autoCloseTry[A <: AutoCloseable, B](closeable: A)(fun: A => B): Try[B] = {
    var t: Option[Throwable] = None
    try {
      Success(fun(closeable))
    } catch {
      case funT: Throwable =>
        t = Option(funT)
        assert(t.isDefined)
        Failure(t.get)
    } finally {
      t.fold(closeable.close()) { throwable =>
        try {
          closeable.close()
        } catch {
          case closeT: Throwable =>
            throwable.addSuppressed(closeT)
            Failure(throwable)
        }
      }
    }
  }

  /** Same as autoCloseTry, but either retrieves the result or throws the throwable */
  def autoCloseTryGet[A <: AutoCloseable, B](closeable: A)(fun: A => B): B = {
    autoCloseTry(closeable)(fun).fold( throwable => throw throwable, batch => batch)
  }

  /** Same as autoCloseTry, but only closes on failure */
  def closeOnFail[A <: AutoCloseable, B](closeable: A)(fun: A => B): Try[B] = {
    var t: Option[Throwable] = None
    try {
      Success(fun(closeable))
    } catch {
      case funT: Throwable =>
        t = Option(funT)
        assert(t.isDefined)
        Failure(t.get)
    } finally {
      t foreach { throwable =>
        try {
          closeable.close()
        } catch {
          case closeT: Throwable =>
            throwable.addSuppressed(closeT)
            Failure(throwable)
        }
      }
    }
  }
}

