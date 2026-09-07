package lab3.task12

import lab3.task12.CommonUtils.KeySeparator

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import java.io.{DataInput, DataOutput}
import java.lang.Iterable
import java.util.Objects

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class MedianSortKey()
    extends WritableComparable[MedianSortKey] {

  private var month: String = ""
  private var state: String = ""
  private var variety: Long = 0L

  def set(
      newMonth: String,
      newState: String,
      newVariety: Long
  ): Unit = {
    month = newMonth
    state = newState
    variety = newVariety
  }

  def getMonth: String = month
  def getState: String = state
  def getVariety: Long = variety

  override def write(output: DataOutput): Unit = {
    Text.writeString(output, month)
    Text.writeString(output, state)
    output.writeLong(variety)
  }

  override def readFields(input: DataInput): Unit = {
    month = Text.readString(input)
    state = Text.readString(input)
    variety = input.readLong()
  }

  override def compareTo(other: MedianSortKey): Int = {
    var comparison = month.compareTo(other.month)
    if (comparison != 0) return comparison

    comparison = state.compareTo(other.state)
    if (comparison != 0) return comparison

    java.lang.Long.compare(
      variety,
      other.variety
    )
  }

  override def equals(other: Any): Boolean = other match {
    case that: MedianSortKey =>
      month == that.month &&
      state == that.state &&
      variety == that.variety

    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(
      month,
      state,
      Long.box(variety)
    )
}

class MedianPartitioner
    extends Partitioner[MedianSortKey, LongWritable] {

  override def getPartition(
      key: MedianSortKey,
      value: LongWritable,
      numPartitions: Int
  ): Int = {
    val hash = Objects.hash(
      key.getMonth,
      key.getState
    )

    Math.floorMod(hash, numPartitions)
  }
}

class MedianGroupingComparator
    extends WritableComparator(
      classOf[MedianSortKey],
      true
    ) {

  override def compare(
      left: WritableComparable[_],
      right: WritableComparable[_]
  ): Int = {
    val a = left.asInstanceOf[MedianSortKey]
    val b = right.asInstanceOf[MedianSortKey]

    var comparison = a.getMonth.compareTo(b.getMonth)
    if (comparison != 0) return comparison

    a.getState.compareTo(b.getState)
  }
}

class MedianMapper
    extends Mapper[
      LongWritable,
      Text,
      MedianSortKey,
      LongWritable
    ] {

  private val outputKey = new MedianSortKey()
  private val outputValue = new LongWritable()

  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[
        LongWritable,
        Text,
        MedianSortKey,
        LongWritable
      ]#Context
  ): Unit = {

    val line = value.toString
    val tabIndex = line.indexOf('\t')

    if (tabIndex <= 0) {
      context
        .getCounter("LAB3_TASK12", "INVALID_STYLE_LINES")
        .increment(1L)
      return
    }

    val compositeKey =
      line.substring(0, tabIndex)

    val varietyText =
      line.substring(tabIndex + 1).trim

    val keyParts =
      compositeKey.split(KeySeparator, -1)

    val varietyOption =
      Try(varietyText.toLong).toOption

    if (
      keyParts.length != 2 ||
      varietyOption.isEmpty
    ) {
      context
        .getCounter("LAB3_TASK12", "INVALID_STYLE_LINES")
        .increment(1L)
      return
    }

    val variety = varietyOption.get

    outputKey.set(
      keyParts(0),
      keyParts(1),
      variety
    )

    outputValue.set(variety)

    context.write(
      outputKey,
      outputValue
    )
  }
}

class MedianReducer
    extends Reducer[
      MedianSortKey,
      LongWritable,
      Text,
      NullWritable
    ] {

  private val outputLine = new Text()

  override def reduce(
      key: MedianSortKey,
      values: Iterable[LongWritable],
      context: Reducer[
        MedianSortKey,
        LongWritable,
        Text,
        NullWritable
      ]#Context
  ): Unit = {

    val orderedVarieties =
      ArrayBuffer.empty[Long]

    val iterator = values.iterator()

    /*
     * Values are already ordered by variety because MedianSortKey
     * sorts on (month, state, variety).
     */
    while (iterator.hasNext) {
      orderedVarieties += iterator.next().get()
    }

    if (orderedVarieties.isEmpty) {
      return
    }

    val count = orderedVarieties.length

    val median =
      if (count % 2 == 1) {
        orderedVarieties(count / 2).toDouble
      } else {
        val left =
          orderedVarieties(count / 2 - 1).toDouble

        val right =
          orderedVarieties(count / 2).toDouble

        (left + right) / 2.0
      }

    outputLine.set(
      Seq(
        key.getMonth,
        key.getState,
        median.toString,
        count.toString
      ).mkString(",")
    )

    context.write(
      outputLine,
      NullWritable.get()
    )
  }
}

object Task12MedianJob {

  def run(
      configuration: Configuration,
      inputPath: Path,
      outputPath: Path
  ): Boolean = {

    val jobConfiguration =
      new Configuration(configuration)

    val fileSystem =
      outputPath.getFileSystem(jobConfiguration)

    if (fileSystem.exists(outputPath)) {
      fileSystem.delete(outputPath, true)
    }

    val job = Job.getInstance(
      jobConfiguration,
      "Lab 3 Task 1-2 Job 2 - Median variety"
    )

    job.setJarByClass(classOf[MedianMapper])
    job.setMapperClass(classOf[MedianMapper])
    job.setPartitionerClass(classOf[MedianPartitioner])
    job.setGroupingComparatorClass(
      classOf[MedianGroupingComparator]
    )
    job.setReducerClass(classOf[MedianReducer])

    job.setMapOutputKeyClass(
      classOf[MedianSortKey]
    )
    job.setMapOutputValueClass(
      classOf[LongWritable]
    )
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])

    // One final part-r file, which can then be copied to a local CSV.
    job.setNumReduceTasks(1)

    FileInputFormat.addInputPath(
      job,
      inputPath
    )

    FileOutputFormat.setOutputPath(
      job,
      outputPath
    )

    job.waitForCompletion(true)
  }
}
