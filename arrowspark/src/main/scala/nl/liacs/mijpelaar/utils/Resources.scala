package nl.liacs.mijpelaar.utils

import scala.language.higherKinds
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
    autoCloseTry(closeable)(fun).fold( throwable => throw throwable, item => item)
  }

  /** Same as autoCloseTry, but for traversables */
  def autoCloseTraversableTry[T <: TraversableOnce[AutoCloseable], B](traversable: T)(fun: T => B): Try[B] = {
    var t: Option[Throwable] = None
    try {
      Success(fun(traversable))
    } catch {
      case funT: Throwable =>
        t = Option(funT)
        assert(t.isDefined)
        Failure(t.get)
    } finally {
      var u: Option[Throwable] = None
      traversable.foreach { item =>
        try {
          item.close()
        } catch {
          case closeT: Throwable => u.fold[Unit]( (u = Option(closeT)) ) ( throwable => throwable.addSuppressed(closeT))
        }
      }
      t.fold( u.foreach( throwable => throw throwable ) )( throwable => u.foreach(throwable.addSuppressed))
    }
  }

  def autoCloseTraversableTryGet[T <: TraversableOnce[AutoCloseable], B](traversable: T)(fun: T => B): B = {
    autoCloseTraversableTry(traversable)(fun).fold( throwable => throw throwable, item => item)
  }

  /** Same as autoCloseTry, but with Option-type */
  def autoCloseOptionTry[T <: Option[AutoCloseable], B](optional: T)(fun: T => B): Try[B] = {
    var t: Option[Throwable] = None
    try {
      Success(fun(optional))
    } catch {
      case funT: Throwable =>
        t = Option(funT)
        assert(t.isDefined)
        Failure(t.get)
    } finally {
      t.fold(optional.foreach(_.close())) { throwable =>
          try {
            optional.foreach(_.close())
          } catch {
            case closeT: Throwable =>
              throwable.addSuppressed(closeT)
              Failure(throwable)
          }
      }
    }
  }

  def autoCloseOptionTryGet[T <: Option[AutoCloseable], B](optional: T)(fun: T => B): B = {
    autoCloseOptionTry(optional)(fun).fold( throwable => throw throwable, item => item)
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

  /** Same as other closeOnFail, but for traversables of closeables */
  def closeTraversableOnFail[T <: TraversableOnce[AutoCloseable], B](traversable: T)(fun: T => B): Try[B] = {
    var t: Option[Throwable] = None
    try {
      Success(fun(traversable))
    } catch {
      case funT: Throwable =>
        t = Option(funT)
        assert(t.isDefined)
        Failure(t.get)
    } finally {
      t foreach { throwable =>
        traversable.foreach { item =>
          try {
           item.close()
          } catch {
            case closeT: Throwable => throwable.addSuppressed(closeT)
          }
        }
      }
    }
  }

  def closeTraversableOnFailGet[T <: TraversableOnce[AutoCloseable], B](traversable: T)(fun: T => B): B = {
    closeTraversableOnFail(traversable)(fun).fold( throwable => throw throwable, item => item)
  }
}

