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


  /** Same as autoCloseTry, but for arrays
   * Note that we possible close multiple times through this function */
  def autoCloseableArrayTry[A <: AutoCloseable, B](array: Array[A])(fun: Array[A] => B): Try[B] = {
    var t: Option[Throwable] = None
    try {
      Success(fun(array))
    } catch {
      case funT: Throwable =>
        t = Option(funT)
        assert(t.isDefined)
        Failure(t.get)
    } finally {
      var u: Option[Throwable] = None
      array.foreach { item =>
        try {
          item.close()
        } catch {
          case closeT: Throwable => u.fold[Unit]( (u = Option(closeT)) ) ( throwable => throwable.addSuppressed(closeT))
        }
      }
      t.fold( u.foreach( throwable => throw throwable ) )( throwable => u.foreach(throwable.addSuppressed))
    }
  }

  def autoCloseArrayTryGet[A <: AutoCloseable, B](array: Array[A])(fun: Array[A] => B): B = {
    autoCloseableArrayTry(array)(fun).fold( throwable => throw throwable, item => item)
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

  def closeOnFailGet[A <: AutoCloseable, B](closeable: A)(fun: A => B): B = {
    closeOnFail(closeable)(fun).fold( throwable => throw throwable, item => item)
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

  /** Same as other closeOnFail, but for arrays
   * Note: we could close the items multiple times because of this */
  def closeArrayOnFail[A <: AutoCloseable, B](array: Array[A])(fun: Array[A] => B): Try[B] = {
    var t: Option[Throwable] = None
    try {
      Success(fun(array))
    } catch {
      case funT: Throwable =>
        t = Option(funT)
        assert(t.isDefined)
        Failure(t.get)
    } finally {
      t foreach { throwable =>
        array.foreach { item =>
          try {
            item.close()
          } catch {
            case closeT: Throwable => throwable.addSuppressed(closeT)
          }
        }
      }
    }
  }

  def closeArrayOnFailGet[A <: AutoCloseable, B](array: Array[A])(fun: Array[A] => B): B = {
    closeArrayOnFail(array)(fun).fold( throwable => throw throwable, item => item)
  }
}

