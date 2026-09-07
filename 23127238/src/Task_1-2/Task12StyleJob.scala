package lab3.task12

import lab3.task12.CommonUtils._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text, Writable, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import java.io.{DataInput, DataOutput}
import java.lang.Iterable
import java.util.Objects

/**
 * Task 1-2 — Job 1: Style Variety Calculation.
 *
 * A style qualifies only if it serves at least one size >= XXL inside
 * the same (month, state) interval whose variety is being measured.
 *
 * Full sort key: (month, state, style, sku)
 * Reducer grouping key: (month, state, style)
 *
 * Because SKU is part of the sort key, the reducer can count distinct
 * SKU values by observing SKU changes; no HashSet is required.
 */

class StyleSkuKey()
    extends WritableComparable[StyleSkuKey] {

  private var month: String = ""
  private var state: String = ""
  private var style: String = ""
  private var sku: String = ""

  def set(
      newMonth: String,
      newState: String,
      newStyle: String,
      newSku: String
  ): Unit = {
    month = newMonth
    state = newState
    style = newStyle
    sku = newSku
  }

  def getMonth: String = month
  def getState: String = state
  def getStyle: String = style
  def getSku: String = sku

  override def write(output: DataOutput): Unit = {
    Text.writeString(output, month)
    Text.writeString(output, state)
    Text.writeString(output, style)
    Text.writeString(output, sku)
  }

  override def readFields(input: DataInput): Unit = {
    month = Text.readString(input)
    state = Text.readString(input)
    style = Text.readString(input)
    sku = Text.readString(input)
  }

  override def compareTo(other: StyleSkuKey): Int = {
    var comparison = month.compareTo(other.month)
    if (comparison != 0) return comparison

    comparison = state.compareTo(other.state)
    if (comparison != 0) return comparison

    comparison = style.compareTo(other.style)
    if (comparison != 0) return comparison

    sku.compareTo(other.sku)
  }

  override def equals(other: Any): Boolean = other match {
    case that: StyleSkuKey =>
      month == that.month &&
      state == that.state &&
      style == that.style &&
      sku == that.sku

    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(month, state, style, sku)
}

/** Mapper value carries the sorted SKU plus the XXL+ flag. */
class StyleValue() extends Writable {

  private var sku: String = ""
  private var hasLargeSize: Boolean = false

  def set(newSku: String, newHasLargeSize: Boolean): Unit = {
    sku = newSku
    hasLargeSize = newHasLargeSize
  }

  def getSku: String = sku
  def getHasLargeSize: Boolean = hasLargeSize

  override def write(output: DataOutput): Unit = {
    Text.writeString(output, sku)
    output.writeBoolean(hasLargeSize)
  }

  override def readFields(input: DataInput): Unit = {
    sku = Text.readString(input)
    hasLargeSize = input.readBoolean()
  }
}

/** Keep each (month, state, style) group on one reducer. */
class StylePartitioner
    extends Partitioner[StyleSkuKey, StyleValue] {

  override def getPartition(
      key: StyleSkuKey,
      value: StyleValue,
      numPartitions: Int
  ): Int = {
    val hash = Objects.hash(
      key.getMonth,
      key.getState,
      key.getStyle
    )

    Math.floorMod(hash, numPartitions)
  }
}

/**
 * Group by (month, state, style), while Hadoop still sorts by SKU
 * because SKU remains in StyleSkuKey.compareTo.
 */
class StyleGroupingComparator
    extends WritableComparator(
      classOf[StyleSkuKey],
      true
    ) {

  override def compare(
      left: WritableComparable[_],
      right: WritableComparable[_]
  ): Int = {
    val a = left.asInstanceOf[StyleSkuKey]
    val b = right.asInstanceOf[StyleSkuKey]

    var comparison = a.getMonth.compareTo(b.getMonth)
    if (comparison != 0) return comparison

    comparison = a.getState.compareTo(b.getState)
    if (comparison != 0) return comparison

    a.getStyle.compareTo(b.getStyle)
  }
}

class StyleMapper
    extends Mapper[
      LongWritable,
      Text,
      StyleSkuKey,
      StyleValue
    ] {

  private val outputKey = new StyleSkuKey()
  private val outputValue = new StyleValue()

  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[
        LongWritable,
        Text,
        StyleSkuKey,
        StyleValue
      ]#Context
  ): Unit = {

    val line = value.toString

    if (isHeader(line)) {
      context
        .getCounter("LAB3_TASK12", "HEADER_ROWS")
        .increment(1L)
      return
    }

    parseCsvLine(line) match {
      case Some(fields)
          if fields.length == ExpectedColumnCount =>

        val state = normalize(fields(Columns.State))
        val style = normalize(fields(Columns.Style))
        val sku = normalize(fields(Columns.SKU))
        val size = normalize(fields(Columns.Size))
        val monthOption = parseMonth(fields(Columns.Date))

        if (state.isEmpty || style.isEmpty || sku.isEmpty) {
          context
            .getCounter("LAB3_TASK12", "MISSING_KEY_FIELDS")
            .increment(1L)
        } else {
          monthOption match {
            case Some(month) =>
              outputKey.set(
                month,
                state,
                style,
                sku
              )

              outputValue.set(
                sku,
                isAtLeastXXL(size)
              )

              context.write(
                outputKey,
                outputValue
              )

              context
                .getCounter("LAB3_TASK12", "VALID_SOURCE_ROWS")
                .increment(1L)

            case None =>
              context
                .getCounter("LAB3_TASK12", "INVALID_DATE_ROWS")
                .increment(1L)
          }
        }

      case Some(_) =>
        context
          .getCounter("LAB3_TASK12", "INVALID_COLUMN_COUNT_ROWS")
          .increment(1L)

      case None =>
        context
          .getCounter("LAB3_TASK12", "INVALID_CSV_ROWS")
          .increment(1L)
    }
  }
}

/**
 * One reduce call corresponds to one (month, state, style).
 * Values arrive in SKU order through the secondary-sort key.
 */
class StyleReducer
    extends Reducer[
      StyleSkuKey,
      StyleValue,
      Text,
      LongWritable
    ] {

  private val outputKey = new Text()
  private val outputVariety = new LongWritable()

  override def reduce(
      key: StyleSkuKey,
      values: Iterable[StyleValue],
      context: Reducer[
        StyleSkuKey,
        StyleValue,
        Text,
        LongWritable
      ]#Context
  ): Unit = {

    val iterator = values.iterator()

    var lastSku: String = null
    var variety = 0L
    var qualifiedInThisGroup = false

    while (iterator.hasNext) {
      val current = iterator.next()
      val sku = current.getSku

      if (lastSku == null || sku != lastSku) {
        variety += 1L
        lastSku = sku
      }

      if (current.getHasLargeSize) {
        qualifiedInThisGroup = true
      }
    }

    if (qualifiedInThisGroup) {
      outputKey.set(
        key.getMonth +
        KeySeparator +
        key.getState
      )

      outputVariety.set(variety)

      context.write(
        outputKey,
        outputVariety
      )

      context
        .getCounter("LAB3_TASK12", "QUALIFIED_STYLES")
        .increment(1L)
    }
  }
}

object Task12StyleJob {

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
      "Lab 3 Task 1-2 Job 1 - Style variety"
    )

    job.setJarByClass(classOf[StyleMapper])
    job.setMapperClass(classOf[StyleMapper])
    job.setPartitionerClass(classOf[StylePartitioner])
    job.setGroupingComparatorClass(
      classOf[StyleGroupingComparator]
    )
    job.setReducerClass(classOf[StyleReducer])

    job.setMapOutputKeyClass(
      classOf[StyleSkuKey]
    )
    job.setMapOutputValueClass(
      classOf[StyleValue]
    )
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])

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
