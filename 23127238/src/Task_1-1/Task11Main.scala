package lab3.task11

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Single-command entry point for Task 1-1.
 *
 * Usage:
 *   Task11Main <input> <work-root> <final-output>
 *
 * The program always runs exactly two MapReduce jobs in sequence:
 *   Job 1/2: count bought orders by state
 *   Job 2/2: sliding-window map-to-buckets + secondary-sort winner selection
 */
object Task11Main {

  private def printUsage(): Unit = {
    System.err.println(
      "Usage: Task11Main <input> <work-root> <final-output>"
    )
  }

  def main(args: Array[String]): Unit = {
    val effectiveArgs =
      if (args.length == 4 && args(0).contains("Task11Main")) args.tail
      else args

    if (effectiveArgs.length != 3) {
      printUsage()
      System.exit(1)
    }

    val configuration = new Configuration()

    val inputPath = new Path(effectiveArgs(0))
    val workRoot = new Path(effectiveArgs(1))
    val finalOutputPath = new Path(effectiveArgs(2))

    val stateCountPath = new Path(
      workRoot,
      "task11-state-count"
    )

    println()
    println("=======================================================")
    println("TASK 1-1 - SINGLE COMMAND / TWO MAPREDUCE JOBS")
    println("=======================================================")
    println(s"Input        = $inputPath")
    println(s"Work root    = $workRoot")
    println(s"State counts = $stateCountPath")
    println(s"Final output = $finalOutputPath")
    println("=======================================================")

    println()
    println("[TASK 1-1] Starting MapReduce Job 1/2: state bought counts")

    val job1Successful = Task11CountJob.run(
      configuration,
      inputPath,
      stateCountPath
    )

    if (!job1Successful) {
      System.err.println("[TASK 1-1] Job 1/2 failed. Job 2/2 will not run.")
      System.exit(1)
    }

    println("[TASK 1-1] MapReduce Job 1/2 completed successfully")
    println()
    println("[TASK 1-1] Starting MapReduce Job 2/2: sliding-window winners")

    val job2Successful = Task11WindowJob.run(
      configuration,
      inputPath,
      stateCountPath,
      finalOutputPath
    )

    if (!job2Successful) {
      System.err.println("[TASK 1-1] Job 2/2 failed.")
      System.exit(1)
    }

    println("[TASK 1-1] MapReduce Job 2/2 completed successfully")
    println()
    println("=======================================================")
    println("TASK 1-1 - SUCCESS: 2/2 MAPREDUCE JOBS COMPLETED")
    println("=======================================================")
  }
}
