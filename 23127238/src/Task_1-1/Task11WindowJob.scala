package lab3.task11

import lab3.task11.CommonUtils._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.filecache.DistributedCache
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text, Writable, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import java.io.{BufferedReader, DataInput, DataOutput, InputStreamReader}
import java.lang.Iterable
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale

import scala.collection.mutable
import scala.util.Try

object Task11WindowConstants {
  val StateCountPathProperty: String = "lab3.task11.state.count.path"
}

/**
 * Composite secondary-sort key:
 *   (state, window_date, size)
 *
 * Natural ordering is exactly the required sort ordering. The reducer grouping
 * comparator later groups only by (state, window_date), so all candidate sizes
 * for a window arrive in one reducer call and remain ordered lexicographically.
 */
class WindowSizeKey()
    extends WritableComparable[WindowSizeKey] {

  private var state: String = ""
  private var windowDate: String = ""
  private var size: String = ""

  def set(newState: String, newWindowDate: String, newSize: String): Unit = {
    state = newState
    windowDate = newWindowDate
    size = newSize
  }

  def getState: String = state
  def getWindowDate: String = windowDate
  def getSize: String = size

  override def write(output: DataOutput): Unit = {
    output.writeUTF(state)
    output.writeUTF(windowDate)
    output.writeUTF(size)
  }

  override def readFields(input: DataInput): Unit = {
    state = input.readUTF()
    windowDate = input.readUTF()
    size = input.readUTF()
  }

  override def compareTo(other: WindowSizeKey): Int = {
    val stateComparison = state.compareTo(other.state)

    if (stateComparison != 0) {
      stateComparison
    } else {
      val dateComparison = windowDate.compareTo(other.windowDate)

      if (dateComparison != 0) {
        dateComparison
      } else {
        size.compareTo(other.size)
      }
    }
  }

  override def toString: String = s"$state$windowDate$size"
}

/**
 * Value carried through map -> combine -> reduce.
 *
 * size is deliberately present in the value as well as the composite key.
 * With reducer grouping by (state, date), the values iterator still arrives in
 * full-key sort order, and carrying size lets the reducer detect size changes.
 */
class WindowStatsWritable()
    extends Writable {

  private var size: String = ""
  private var frequency: Long = 0L
  private var amountCount: Long = 0L
  private var amountSum: Double = 0.0
  private var amountSquareSum: Double = 0.0
  private var windowDays: Int = 0

  def set(
      newSize: String,
      newFrequency: Long,
      newAmountCount: Long,
      newAmountSum: Double,
      newAmountSquareSum: Double,
      newWindowDays: Int
  ): Unit = {
    size = newSize
    frequency = newFrequency
    amountCount = newAmountCount
    amountSum = newAmountSum
    amountSquareSum = newAmountSquareSum
    windowDays = newWindowDays
  }

  def getSize: String = size
  def getFrequency: Long = frequency
  def getAmountCount: Long = amountCount
  def getAmountSum: Double = amountSum
  def getAmountSquareSum: Double = amountSquareSum
  def getWindowDays: Int = windowDays

  override def write(output: DataOutput): Unit = {
    output.writeUTF(size)
    output.writeLong(frequency)
    output.writeLong(amountCount)
    output.writeDouble(amountSum)
    output.writeDouble(amountSquareSum)
    output.writeInt(windowDays)
  }

  override def readFields(input: DataInput): Unit = {
    size = input.readUTF()
    frequency = input.readLong()
    amountCount = input.readLong()
    amountSum = input.readDouble()
    amountSquareSum = input.readDouble()
    windowDays = input.readInt()
  }
}

/** Partition all sizes of the same (state, date) to the same reducer. */
class StateDatePartitioner
    extends Partitioner[WindowSizeKey, WindowStatsWritable] {

  override def getPartition(
      key: WindowSizeKey,
      value: WindowStatsWritable,
      numPartitions: Int
  ): Int = {
    val hash = 31 * key.getState.hashCode + key.getWindowDate.hashCode
    (hash & Int.MaxValue) % numPartitions
  }
}

/** Reducer groups all sizes belonging to the same (state, window_date). */
class StateDateGroupingComparator
    extends WritableComparator(classOf[WindowSizeKey], true) {

  override def compare(
      left: WritableComparable[_],
      right: WritableComparable[_]
  ): Int = {
    val a = left.asInstanceOf[WindowSizeKey]
    val b = right.asInstanceOf[WindowSizeKey]

    val stateComparison = a.getState.compareTo(b.getState)
    if (stateComparison != 0) {
      stateComparison
    } else {
      a.getWindowDate.compareTo(b.getWindowDate)
    }
  }
}

/**
 * Combiner must NOT use the reducer's (state,date) grouping comparator.
 * It must combine only identical (state,date,size) keys.
 */
class FullWindowSizeGroupingComparator
    extends WritableComparator(classOf[WindowSizeKey], true) {

  override def compare(
      left: WritableComparable[_],
      right: WritableComparable[_]
  ): Int = {
    left
      .asInstanceOf[WindowSizeKey]
      .compareTo(right.asInstanceOf[WindowSizeKey])
  }
}

/**
 * Task 1-1 - Job 2/2 mapper.
 *
 * A bought order on date t is mapped to all future result buckets that include
 * it in their past-only window: t+1 ... t+L. This is exactly equivalent to a
 * result window [d-L, d-1] and therefore never includes day d itself.
 */
class WindowBucketMapper
    extends Mapper[LongWritable, Text, WindowSizeKey, WindowStatsWritable] {

  private val inputDateFormatter =
    DateTimeFormatter.ofPattern("MM-dd-yy", Locale.ROOT)

  private val stateWindowDays = mutable.HashMap.empty[String, Int]
  private val outputKey = new WindowSizeKey()
  private val outputStats = new WindowStatsWritable()

  override def setup(
      context: Mapper[LongWritable, Text, WindowSizeKey, WindowStatsWritable]#Context
  ): Unit = {
    val configuration = context.getConfiguration

    val localizedFiles = Option(
      DistributedCache.getLocalCacheFiles(configuration)
    ).getOrElse(Array.empty[Path])

    if (localizedFiles.nonEmpty) {
      localizedFiles.foreach { localPath =>
        loadStateCountFile(FileSystem.getLocal(configuration), localPath)
      }
    } else {
      // Defensive fallback. The job still registers the lookup through
      // addCacheFile; this fallback makes local/pseudo-distributed setups robust.
      val stateCountPathText = configuration.get(
        Task11WindowConstants.StateCountPathProperty
      )

      if (stateCountPathText == null || stateCountPathText.trim.isEmpty) {
        throw new IllegalStateException("No state-count lookup is available")
      }

      val stateCountPath = new Path(stateCountPathText)
      val fileSystem = stateCountPath.getFileSystem(configuration)

      fileSystem
        .listStatus(stateCountPath)
        .filter(status => status.isFile && status.getPath.getName.startsWith("part-"))
        .foreach(status => loadStateCountFile(fileSystem, status.getPath))
    }

    if (stateWindowDays.isEmpty) {
      throw new IllegalStateException("The state-count lookup table is empty")
    }
  }

  private def loadStateCountFile(fileSystem: FileSystem, path: Path): Unit = {
    val reader = new BufferedReader(
      new InputStreamReader(fileSystem.open(path), StandardCharsets.UTF_8)
    )

    try {
      var line = reader.readLine()

      while (line != null) {
        val parts = line.split("\t", -1)

        if (parts.length == 2) {
          val state = normalize(parts(0))
          val countOption = Try(parts(1).trim.toLong).toOption

          countOption.foreach { count =>
            stateWindowDays.put(
              state,
              if (count > 10000L) 5 else 10
            )
          }
        }

        line = reader.readLine()
      }
    } finally {
      reader.close()
    }
  }

  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, WindowSizeKey, WindowStatsWritable]#Context
  ): Unit = {

    val line = value.toString

    if (isHeader(line)) {
      context.getCounter("LAB3_TASK11", "HEADER_ROWS_JOB2").increment(1L)
      return
    }

    parseCsvLine(line) match {
      case Some(fields) if fields.length == ExpectedColumnCount =>
        val state = normalize(fields(Columns.State))
        val size = normalize(fields(Columns.Size))
        val status = fields(Columns.Status)
        val qtyOption = parseInt(fields(Columns.Qty))

        stateWindowDays.get(state) match {
          case None =>
            if (state.nonEmpty) {
              context
                .getCounter("LAB3_TASK11", "STATE_NOT_IN_COUNT_TABLE_JOB2")
                .increment(1L)
            }

          case Some(windowDays) =>
            qtyOption match {
              case Some(qty) if isBought(status, qty) =>
                if (size.isEmpty) {
                  context.getCounter("LAB3_TASK11", "MISSING_SIZE_ROWS_JOB2").increment(1L)
                } else {
                  val dateOption = Try(
                    LocalDate.parse(fields(Columns.Date).trim, inputDateFormatter)
                  ).toOption

                  dateOption match {
                    case None =>
                      context.getCounter("LAB3_TASK11", "INVALID_DATE_ROWS_JOB2").increment(1L)

                    case Some(orderDate) =>
                      val amountOption = parseDouble(fields(Columns.Amount))
                      val amountCount = if (amountOption.isDefined) 1L else 0L
                      val amount = amountOption.getOrElse(0.0)
                      val amountSquare = amount * amount

                      var offset = 1

                      while (offset <= windowDays) {
                        val resultDate = orderDate.plusDays(offset.toLong)

                        outputKey.set(state, resultDate.toString, size)
                        outputStats.set(
                          size,
                          1L,
                          amountCount,
                          amount,
                          amountSquare,
                          windowDays
                        )

                        context.write(outputKey, outputStats)
                        offset += 1
                      }

                      context.getCounter("LAB3_TASK11", "BUCKET_SOURCE_ROWS").increment(1L)
                      context
                        .getCounter("LAB3_TASK11", "BUCKET_RECORDS_EMITTED")
                        .increment(windowDays.toLong)

                      if (amountOption.isEmpty) {
                        context
                          .getCounter("LAB3_TASK11", "BOUGHT_ROWS_WITH_NULL_AMOUNT")
                          .increment(1L)
                      }
                  }
                }

              case _ =>
                // Non-bought rows do not belong to any window bucket.
            }
        }

      case Some(_) =>
        context.getCounter("LAB3_TASK11", "WRONG_COLUMN_COUNT_ROWS_JOB2").increment(1L)

      case None =>
        context.getCounter("LAB3_TASK11", "INVALID_CSV_ROWS_JOB2").increment(1L)
    }
  }
}

/**
 * Local aggregation for identical (state,date,size) keys.
 * The separate combiner grouping comparator guarantees that different sizes
 * are never mixed here.
 */
class WindowStatsCombiner
    extends Reducer[WindowSizeKey, WindowStatsWritable, WindowSizeKey, WindowStatsWritable] {

  private val outputStats = new WindowStatsWritable()

  override def reduce(
      key: WindowSizeKey,
      values: Iterable[WindowStatsWritable],
      context: Reducer[
        WindowSizeKey,
        WindowStatsWritable,
        WindowSizeKey,
        WindowStatsWritable
      ]#Context
  ): Unit = {

    val iterator = values.iterator()
    var frequency = 0L
    var amountCount = 0L
    var amountSum = 0.0
    var amountSquareSum = 0.0
    var windowDays = 0

    while (iterator.hasNext) {
      val current = iterator.next()
      frequency += current.getFrequency
      amountCount += current.getAmountCount
      amountSum += current.getAmountSum
      amountSquareSum += current.getAmountSquareSum
      windowDays = math.max(windowDays, current.getWindowDays)
    }

    outputStats.set(
      key.getSize,
      frequency,
      amountCount,
      amountSum,
      amountSquareSum,
      windowDays
    )

    context.write(key, outputStats)
  }
}

final case class WindowCandidate(
    size: String,
    frequency: Long,
    variance: Double,
    amountCount: Long,
    windowDays: Int
)

/**
 * One reducer call = one (state, window_date), with values ordered by size.
 * The reducer aggregates each size, applies the two tie-break rules, and emits
 * the final winner directly, eliminating the old third WinnerJob.
 */
class WindowWinnerReducer
    extends Reducer[WindowSizeKey, WindowStatsWritable, Text, NullWritable] {

  private val outputLine = new Text()

  private def populationVariance(
      amountCount: Long,
      amountSum: Double,
      amountSquareSum: Double
  ): Double = {
    if (amountCount == 0L) {
      Double.PositiveInfinity
    } else {
      val n = amountCount.toDouble
      math.max(
        0.0,
        amountSquareSum / n - math.pow(amountSum / n, 2.0)
      )
    }
  }

  private def isBetter(
      candidate: WindowCandidate,
      currentBest: WindowCandidate
  ): Boolean = {
    if (candidate.frequency != currentBest.frequency) {
      candidate.frequency > currentBest.frequency
    } else {
      val varianceComparison = java.lang.Double.compare(
        candidate.variance,
        currentBest.variance
      )

      if (varianceComparison != 0) {
        varianceComparison < 0
      } else {
        compareSizeLexicographically(candidate.size, currentBest.size) < 0
      }
    }
  }

  override def reduce(
      key: WindowSizeKey,
      values: Iterable[WindowStatsWritable],
      context: Reducer[WindowSizeKey, WindowStatsWritable, Text, NullWritable]#Context
  ): Unit = {

    val state = key.getState
    val windowDate = key.getWindowDate
    val iterator = values.iterator()

    var currentSize: String = null
    var currentFrequency = 0L
    var currentAmountCount = 0L
    var currentAmountSum = 0.0
    var currentAmountSquareSum = 0.0
    var currentWindowDays = 0

    var bestCandidate: WindowCandidate = null
    var topFrequency = Long.MinValue
    var topFrequencySizeCount = 0

    def finalizeCurrentSize(): Unit = {
      if (currentSize != null) {
        val candidate = WindowCandidate(
          currentSize,
          currentFrequency,
          populationVariance(
            currentAmountCount,
            currentAmountSum,
            currentAmountSquareSum
          ),
          currentAmountCount,
          currentWindowDays
        )

        if (candidate.frequency > topFrequency) {
          topFrequency = candidate.frequency
          topFrequencySizeCount = 1
        } else if (candidate.frequency == topFrequency) {
          topFrequencySizeCount += 1
        }

        if (bestCandidate == null || isBetter(candidate, bestCandidate)) {
          bestCandidate = candidate
        }
      }
    }

    while (iterator.hasNext) {
      val current = iterator.next()
      val size = current.getSize

      if (currentSize == null) {
        currentSize = size
      } else if (size != currentSize) {
        finalizeCurrentSize()

        currentSize = size
        currentFrequency = 0L
        currentAmountCount = 0L
        currentAmountSum = 0.0
        currentAmountSquareSum = 0.0
        currentWindowDays = 0
      }

      currentFrequency += current.getFrequency
      currentAmountCount += current.getAmountCount
      currentAmountSum += current.getAmountSum
      currentAmountSquareSum += current.getAmountSquareSum
      currentWindowDays = math.max(currentWindowDays, current.getWindowDays)
    }

    finalizeCurrentSize()

    if (topFrequencySizeCount >= 2) {
      context
        .getCounter("LAB3_TASK11", "TOP_FREQUENCY_TIE_GROUPS")
        .increment(1L)
    }

    if (bestCandidate != null) {
      outputLine.set(
        Seq(
          state,
          windowDate,
          bestCandidate.size,
          bestCandidate.frequency.toString,
          bestCandidate.variance.toString,
          bestCandidate.amountCount.toString,
          bestCandidate.windowDays.toString
        ).mkString(",")
      )

      context.write(outputLine, NullWritable.get())
    }
  }
}

object Task11WindowJob {

  def run(
      configuration: Configuration,
      inputPath: Path,
      stateCountPath: Path,
      outputPath: Path
  ): Boolean = {

    val jobConfiguration = new Configuration(configuration)
    jobConfiguration.set(
      Task11WindowConstants.StateCountPathProperty,
      stateCountPath.toString
    )

    val outputFileSystem = outputPath.getFileSystem(jobConfiguration)
    if (outputFileSystem.exists(outputPath)) {
      outputFileSystem.delete(outputPath, true)
    }

    val stateCountFileSystem = stateCountPath.getFileSystem(jobConfiguration)
    val stateCountPartFiles = stateCountFileSystem
      .listStatus(stateCountPath)
      .filter(status => status.isFile && status.getPath.getName.startsWith("part-"))

    if (stateCountPartFiles.isEmpty) {
      throw new IllegalStateException(
        s"No state-count part file found under $stateCountPath"
      )
    }

    val job = Job.getInstance(
      jobConfiguration,
      "Lab 3 Task 1-1 Job 2/2 - Sliding-window winners with secondary sort"
    )

    stateCountPartFiles.foreach(status => job.addCacheFile(status.getPath.toUri))

    job.setJarByClass(classOf[WindowBucketMapper])
    job.setMapperClass(classOf[WindowBucketMapper])
    job.setCombinerClass(classOf[WindowStatsCombiner])
    job.setReducerClass(classOf[WindowWinnerReducer])

    job.setPartitionerClass(classOf[StateDatePartitioner])

    // Reducer groups by (state,date), while the combiner must retain full
    // (state,date,size) grouping to avoid mixing candidate sizes.
    job.setGroupingComparatorClass(classOf[StateDateGroupingComparator])
    job.setCombinerKeyGroupingComparatorClass(classOf[FullWindowSizeGroupingComparator])

    job.setMapOutputKeyClass(classOf[WindowSizeKey])
    job.setMapOutputValueClass(classOf[WindowStatsWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])

    // A single final part file is convenient for the required single CSV export.
    job.setNumReduceTasks(1)

    FileInputFormat.addInputPath(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)

    job.waitForCompletion(true)
  }
}
