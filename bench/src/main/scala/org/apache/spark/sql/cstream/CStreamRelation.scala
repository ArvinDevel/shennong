package org.apache.spark.sql.cstream

import java.util.concurrent.TimeUnit
import java.util.{ArrayList, Arrays}

import scala.collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.avro.generic.GenericRecord
import org.apache.bookkeeper.api.stream.{ColumnReaderConfig, Position, StreamConfig}
import org.apache.bookkeeper.clients.impl.stream.event.EventPositionImpl
import org.apache.bookkeeper.common.concurrent.FutureUtils

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
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

    val streamConfig = StreamConfig.builder[Array[Byte], GenericRecord]().build()
    val crConfig = ColumnReaderConfig.builder()
      .columns(new ArrayList(Arrays.asList(requiredColumns: _*))).
      maxReadAheadCacheSize(options.readAheadCacheSize).build()

    val stream = FutureUtils.result(
      CachedStorageClient.getOrCreate(ClientConfig(options.url, options.namespace))
        .openStream(options.streamName, streamConfig))
    val columnReader = FutureUtils.result(stream.openColumnReader(crConfig, start, end))
    val cvs = columnReader.readNextVector()

    new Iterator[InternalRow] {

      private var curRoot: VectorSchemaRoot = null
      private var rowIter = if (cvs.hasNext) nextBatch() else Iterator.empty

      override def hasNext: Boolean = rowIter.hasNext || {
        if (cvs.hasNext) {
          rowIter = nextBatch()
          if (rowIter.isEmpty) false else true
        } else {
          if (curRoot != null) {
            curRoot.close()
          }
          false
        }
      }

      override def next(): InternalRow = rowIter.next()

      private def nextBatch(): Iterator[InternalRow] = {
        if (curRoot != null) {
          curRoot.close()
        }
        val cv = cvs.next(options.pollTimeoutMs, TimeUnit.MILLISECONDS)
        if (cv == null) {
          return Iterator.empty
        }
        curRoot = cv.value()
        val cols = curRoot.getFieldVectors.asScala.map { vector =>
          new ArrowColumnVector(vector).asInstanceOf[ColumnVector]
        }.toArray

        val batch = new ColumnarBatch(cols)
        batch.setNumRows(curRoot.getRowCount)
        batch.rowIterator().asScala
      }
    }
  }

  case class RangeSE(id: Long, start: EventPositionImpl, end: EventPositionImpl) {
    def length = end.getRangeSeqNum - start.getRangeSeqNum
  }

  override protected def getPartitions: Array[Partition] = {
    val (start, end) = Utils.tryWithResource(new MetadataReader(options)) { reader =>
      (reader.getStartPos(), reader.getEndPos())
    }

    val rangesStart = start.getRangePositions.asScala
    val rangesEnd = end.getRangePositions.asScala

    val ranges = rangesStart.keys.map { rid =>
      RangeSE(rid,
        rangesStart.getOrElse(rid, throw new RuntimeException),
        rangesEnd.getOrElse(rid, throw new RuntimeException(s"Range $rid doesn't have an end")))
    }

    val totalLength = ranges.foldLeft(0L)((accum, element) => accum + element.length)
    val perPartitionLength = totalLength / options.parallelism

    val cspartitions = scala.collection.mutable.ListBuffer.empty[CStreamPartition]

    var idx: Int = 0
    ranges.foreach { range =>
      val rangeId = range.start.getRangeId

      if (range.length < perPartitionLength * 1.5) {
        cspartitions += CStreamPartition(range.start, range.end, idx)
        idx += 1
      } else {
        var last = false
        var cur = range.start.getRangeSeqNum
        var left = range.length
        while(!last) {
          if (left < perPartitionLength * 1.5) {
            cspartitions += CStreamPartition(
              EventPositionImpl.of(rangeId, 0, cur, 0),
              range.end, idx)
            idx += 1
            left = 0;
            cur = range.end.getRangeSeqNum
            last = true
          } else {
            val curEnd = cur + perPartitionLength
            cspartitions += CStreamPartition(
              EventPositionImpl.of(rangeId, 0, cur, 0),
              EventPositionImpl.of(rangeId, 0, curEnd, 0), idx)
            cur = curEnd
            idx += 1
            left = range.end.getRangeSeqNum - cur
          }
        }
      }
    }
    cspartitions.toArray
  }
}

case class CStreamPartition(start: Position, end: Position, index: Int) extends Partition
