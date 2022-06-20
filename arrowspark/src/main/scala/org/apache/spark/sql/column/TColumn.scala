package org.apache.spark.sql.column

import org.apache.spark.sql.column.expressions.GenericColumn

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

object TColumn {
  /** This method can be used to extract values from a TColumn object */
  def unapplySeq(col: TColumn): Seq[Option[Any]] = col.toSeq

  /** This method can be used to construct a TColumn with the given values */
  def apply[T](values: T*)(implicit tag: ClassTag[T]): TColumn = new GenericColumn(values.toArray.asInstanceOf[Array[Any]])

  /** This method can be used to construct a TColumn from a Seq of values */
  def fromSeq[T](values: Seq[T])(implicit tag: ClassTag[T]): TColumn = new GenericColumn(values.toArray.asInstanceOf[Array[Any]])

  /** This method can be used to construct an Array of TColumns from an Array of ColumnBatches */
  def fromBatches(batches: Array[ColumnBatch]): Array[TColumn] = {
    val columns: ArrayBuffer[TColumn] = new ArrayBuffer()

    batches foreach { batch =>
      0 until batch.length foreach { i =>
        if (i >= columns.length)
          columns += TColumn.empty

        if (!batch.isNullAt(i))
          columns(i) = columns(i).concat(batch.getColumn(i).get)
      }
    }

    columns.toArray
  }

  /** Returns an empty TColumn */
  val empty: TColumn = apply()
}

/** Note: inspiration from: Row.scala */
/** Note: T stands for Trait ;)
 * Unfortunately, spark already had a Column class*/
trait TColumn extends Serializable {
  /** Type of the column */
  def colType: Class[_]

  /** Number of elements in the Column */
  def size: Int = length

  /** Number of elements in the Column */
  def length: Int

  /** Returns the value at position i. If the value is null, None is returned.
   * Verifies if type is equal to colType, or returns None */
  def apply(i: Int): Option[Any] = get(i)

  /** Returns the value at position i. If the value is null, None is returned
   * Verifies type is equal to colType, or returns None */
  def get(i: Int): Option[Any] = {
    getInternal(i).flatMap{ value =>
      if (ClassTag(value.getClass) != ClassTag(colType)) None else Some(value)
    }
  }

  /** Returns the value at position i. If the value is null, None is returned */
  protected def getInternal(i: Int): Option[Any]

  /** Checks whether the value at position i is null */
  def isNullAt(i: Int): Boolean = get(i).isDefined

  override def toString: String = this.mkString("{", ",", "}")

  /** Make a copy of the current Column object */
  def copy(): TColumn

  /** Returns true if there are any NULL values in this row */
  def anyNull: Boolean = 0 until length exists (i => isNullAt(i))

  /** Concats this TColumn with an other TColumn and returns the result */
  def concat(other: TColumn): TColumn

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[TColumn]) return false
    val other = o.asInstanceOf[TColumn]

    if (other eq null) return false
    if (length != other.length) return false
    (0 until length) forall (i => get(i) == other.get(i))
  }

  override def hashCode(): Int = {
    // Using Scala's Seq hash code implementation
    var h = MurmurHash3.seqSeed
    0 until length foreach( i => h = MurmurHash3.mix(h, apply(i).##))
    MurmurHash3.finalizeHash(h, length)
  }

  /* ---------------------- utility methods for Scala ---------------------- */
  /** Returns an iterator over de values of the columns */
  def getIterator: Iterator[Option[Any]] = new Iterator[Option[Any]] {
    private var index = 0
    override def hasNext: Boolean = index < length

    override def next(): Option[Any] = {
      val value = get(index)
      index += 1
      value
    }
  }

  /** Return a Scala Seq representing the Column.
   *  Elements are placed in the same order in the Seq */
  def toSeq: Seq[Option[Any]] = Seq.tabulate(length)( i => get(i) )

  /** Displays all elements of this sequence in a string */
  def mkString: String = mkString("")

  /** Displays all elements of this sequence in a string using a separator string */
  def mkString(sep: String): String = mkString("", sep, "")

  /** Displays all elements of this sequence in a string
   * using start, end, and separator string */
  def mkString(start: String, sep: String, end: String): String = {
    val builder = new mutable.StringBuilder(start)
    if (length > 0) builder.append(get(0))
    1 until length foreach { i =>
      builder.append(sep)
      builder.append(get(i))
    }
    builder.append(end)
    builder.toString()
  }



  /** Note: perhaps in the future, json functionalities will be supported here */
}
