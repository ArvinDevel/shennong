package org.apache.spark.sql.cstream

import org.apache.bookkeeper.api.stream.Position
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils


class CStreamRelation(
    override val sqlContext: SQLContext,
    override val schema: StructType,
    options: CStreamOptions)
  extends BaseRelation with PrunedScan {

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val rdd = new CStreamRDD(sqlContext.sparkContext, schema, requiredColumns, options)
    sqlContext.internalCreateDataFrame(rdd.setName("cstream"), schema).rdd
  }
}

class CStreamRDD(
    sc: SparkContext,
    schema: StructType,
    requiredColumns: Array[String],
    options: CStreamOptions)
  extends RDD[InternalRow](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val part = split.asInstanceOf[CStreamPartition]
    val start = part.start
    val end = part.end



  }

  override protected def getPartitions: Array[Partition] = {
    val partitions = new Array[Partition](options.parallelism)
    val (start, end, numEntry) = Utils.tryWithResource(new MetadataReader(options)) { reader =>
      (reader.getStartPos(), reader.getEndPos(), reader.getEntryCount())
    }

    val entryInEachPartition = numEntry / partitions.size

    var currentPos = start
    for (i <- 0 until partitions.size) {
      val curEnd = positionAdd(currentPos, entryInEachPartition)
      partitions(i) = CStreamPartition(currentPos, curEnd, i)
      currentPos = curEnd
    }
    partitions
  }

  // get end position
  private def positionAdd(start: Position, num: Long): Position = ???
}

case class CStreamPartition(start: Position, end: Position, index: Int) extends Partition
