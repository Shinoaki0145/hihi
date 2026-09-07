package lab3.task11

import lab3.task11.CommonUtils._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import java.lang.Iterable

/**
 * Task 1-1 - Job 1/2
 *
 * Count bought orders per normalized state over the whole dataset.
 * The small result is later distributed to Job 2 so each mapper can choose
 * a 5-day or 10-day window for its state.
 */
class BoughtCountMapper
    extends Mapper[LongWritable, Text, Text, LongWritable] {

  private val outputState = new Text()
  private val one = new LongWritable(1L)

  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, LongWritable]#Context
  ): Unit = {

    val line = value.toString

    if (isHeader(line)) {
      context.getCounter("LAB3_TASK11", "HEADER_ROWS_JOB1").increment(1L)
      return
    }

    parseCsvLine(line) match {
      case Some(fields) if fields.length == ExpectedColumnCount =>
        val state = normalize(fields(Columns.State))
        val status = fields(Columns.Status)
        val qtyOption = parseInt(fields(Columns.Qty))

        if (state.isEmpty) {
          context.getCounter("LAB3_TASK11", "MISSING_STATE_ROWS_JOB1").increment(1L)
        } else {
          qtyOption match {
            case Some(qty) if isBought(status, qty) =>
              outputState.set(state)
              context.write(outputState, one)
              context.getCounter("LAB3_TASK11", "BOUGHT_ROWS_JOB1").increment(1L)

            case Some(_) =>
              context.getCounter("LAB3_TASK11", "NOT_BOUGHT_ROWS_JOB1").increment(1L)

            case None =>
              context.getCounter("LAB3_TASK11", "INVALID_QTY_ROWS_JOB1").increment(1L)
          }
        }

      case Some(_) =>
        context.getCounter("LAB3_TASK11", "WRONG_COLUMN_COUNT_ROWS_JOB1").increment(1L)

      case None =>
        context.getCounter("LAB3_TASK11", "INVALID_CSV_ROWS_JOB1").increment(1L)
    }
  }
}

class BoughtCountReducer
    extends Reducer[Text, LongWritable, Text, LongWritable] {

  private val outputCount = new LongWritable()

  override def reduce(
      key: Text,
      values: Iterable[LongWritable],
      context: Reducer[Text, LongWritable, Text, LongWritable]#Context
  ): Unit = {

    val iterator = values.iterator()
    var total = 0L

    while (iterator.hasNext) {
      total += iterator.next().get()
    }

    outputCount.set(total)
    context.write(key, outputCount)
  }
}

object Task11CountJob {

  def run(
      configuration: Configuration,
      inputPath: Path,
      outputPath: Path
  ): Boolean = {

    val jobConfiguration = new Configuration(configuration)
    val fileSystem = outputPath.getFileSystem(jobConfiguration)

    if (fileSystem.exists(outputPath)) {
      fileSystem.delete(outputPath, true)
    }

    val job = Job.getInstance(
      jobConfiguration,
      "Lab 3 Task 1-1 Job 1/2 - Count bought orders by state"
    )

    job.setJarByClass(classOf[BoughtCountMapper])
    job.setMapperClass(classOf[BoughtCountMapper])
    job.setCombinerClass(classOf[BoughtCountReducer])
    job.setReducerClass(classOf[BoughtCountReducer])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[LongWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])

    // A single part file makes the small state lookup straightforward to cache.
    job.setNumReduceTasks(1)

    FileInputFormat.addInputPath(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)

    job.waitForCompletion(true)
  }
}
