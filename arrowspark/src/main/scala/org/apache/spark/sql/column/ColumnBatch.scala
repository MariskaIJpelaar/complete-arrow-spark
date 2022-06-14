package org.apache.spark.sql.column

import org.apache.spark.sql.column.expressions.GenericColumnBatch

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

object ColumnBatch {
  /** This method can be used to extract values from a ColumnBatch object */
  def unapplySeq(cols: ColumnBatch): Seq[Option[TColumn]] = cols.toSeq

  /** This method can be used to construct a ColumnBatch with given values */
  def apply(values: TColumn*): ColumnBatch = new GenericColumnBatch(values.toArray)

  /** This method can be used to construct a ColumnBatch from a Seq of values */
  def fromSeq(values: Seq[TColumn]): ColumnBatch = new GenericColumnBatch(values.toArray)

  /** Returns an empty ColumnBatch */
  val empty: ColumnBatch = apply()
}

trait ColumnBatch extends Serializable {
  /** Number of Columns */
  def size: Int = length

  /** Number of Columns */
  def length: Int

  /** Returns the i-th column. If the value is null, None is returned */
  def apply(i: Int): Option[TColumn] = get(i)

  /** Returns the i-th column. If the value is null, None is returned */
  def get(i: Int): Option[TColumn] = { getInternal(i) }

  /** Returns the i-th column. If the value is null, None is returned */
  protected def getInternal(i: Int): Option[TColumn]

  /** Checks whether the value at position i is null */
  def isNullAt(i: Int): Boolean = get(i).isDefined

  override def toString: String = this.mkString("[", ";", "]")

  /** Make a copy of the current object */
  def copy(): ColumnBatch

  /** Returns true if there are any NULL values in this row */
  def anyNull: Boolean = 0 until length exists (i => isNullAt(i))

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[ColumnBatch]) return false
    val other = o.asInstanceOf[ColumnBatch]

    if (other eq null) return false
    if (length != other.length) return false
    (0 until length) forall (i => get(i).equals(other.get(i)))
  }

  override def hashCode(): Int = {
    // Using Scala's Seq hash code implementation
    var h = MurmurHash3.seqSeed
    0 until length foreach( i => h = MurmurHash3.mix(h, apply(i).##))
    MurmurHash3.finalizeHash(h, length)
  }

  /* ---------------------- utility methods for Scala ---------------------- */
  /** Return a Scala Seq representing the Column.
   *  Elements are placed in the same order in the Seq */
  def toSeq: Seq[Option[TColumn]] = Seq.tabulate(length)( i => get(i) )

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
      builder.append(get(i).toString)
    }
    builder.append(end)
    builder.toString()
  }



  /** Note: perhaps in the future, json functionalities will be supported here */

}
