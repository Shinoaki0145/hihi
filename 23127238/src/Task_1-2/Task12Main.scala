package lab3.task12

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Runs Task 1-2 as exactly two sequential MapReduce jobs from one command:
 *
 *   Job 1: calculate qualified style variety per (month, state)
 *   Job 2: calculate median variety per (month, state)
 */
object Task12Main {

  private def printUsage(): Unit = {
    System.err.println(
      "Usage: Task12Main <input> <work-root> <final-output>"
    )
  }

  def main(args: Array[String]): Unit = {
    val effectiveArgs =
      if (args.length == 4 && args(0).contains("Task12Main")) args.tail
      else args

    if (effectiveArgs.length != 3) {
      printUsage()
      System.exit(1)
    }

    val configuration = new Configuration()

    val inputPath = new Path(effectiveArgs(0))
    val workRoot = new Path(effectiveArgs(1))
    val finalPath = new Path(effectiveArgs(2))

    val stylePath = new Path(
      workRoot,
      "task12-style-variety"
    )

    println("=======================================================")
    println("TASK 1-2 - START")
    println("=======================================================")
    println(s"Input        = $inputPath")
    println(s"Work root    = $workRoot")
    println(s"Style output = $stylePath")
    println(s"Final output = $finalPath")
    println("=======================================================")

    println()
    println("[TASK 1-2] Starting MapReduce Job 1/2: style variety")

    val job1Successful = Task12StyleJob.run(
      configuration,
      inputPath,
      stylePath
    )

    if (!job1Successful) {
      System.err.println("[TASK 1-2] Job 1/2 failed. Job 2 will not run.")
      System.exit(1)
    }

    println("[TASK 1-2] MapReduce Job 1/2 completed successfully")
    println()
    println("[TASK 1-2] Starting MapReduce Job 2/2: median variety")

    val job2Successful = Task12MedianJob.run(
      configuration,
      stylePath,
      finalPath
    )

    if (!job2Successful) {
      System.err.println("[TASK 1-2] Job 2/2 failed.")
      System.exit(1)
    }

    println("[TASK 1-2] MapReduce Job 2/2 completed successfully")
    println("=======================================================")
    println("TASK 1-2 - SUCCESS: 2/2 JOBS COMPLETED")
    println("=======================================================")
  }
}
