package lab3.task22

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Task22Main {

  private val RequiredColumns = Seq(
    "index",
    "Date",
    "SKU",
    "Amount",
    "promotion-ids"
  )

  private val BenchmarkRuns = 5
  private val ApproxAccuracy = 10000
  private val CompareTolerance = 1e-9

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: Task22Main <input-csv> <output-directory>")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession.builder()
      .appName("Lab 3 Task 2-2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      val raw = spark.read
        .option("header", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("mode", "PERMISSIVE")
        .csv(inputPath)

      val missingColumns = RequiredColumns.filterNot(raw.columns.contains)

      require(
        missingColumns.isEmpty,
        s"Missing columns: ${missingColumns.mkString(", ")}"
      )

      require(
        raw.columns.length == 24,
        s"Expected 24 CSV columns after parsing, found ${raw.columns.length}."
      )

      val promotionArray = split(
        coalesce(col("promotion-ids"), lit("")),
        ","
      )

      val prepared = raw
        .select(
          col("index").cast("long").as("row_id"),
          to_date(trim(col("Date")), "MM-dd-yy").as("order_date"),
          trim(col("SKU")).as("sku"),
          col("Amount").cast("double").as("amount"),
          size(
            filter(
              promotionArray,
              x => length(trim(x)) > 0
            )
          ).cast("int").as("promotion_count")
        )
        .withColumn("month", date_format(col("order_date"), "yyyy-MM"))
        .filter(
          col("row_id").isNotNull &&
          col("order_date").isNotNull &&
          col("sku").isNotNull &&
          length(col("sku")) > 0
        )
        .select(
          "row_id",
          "sku",
          "month",
          "amount",
          "promotion_count"
        )
        .persist(StorageLevel.MEMORY_AND_DISK)

      val preparedRowCount = prepared.count()

      val duplicateRowIdCount = prepared
        .groupBy("row_id")
        .count()
        .filter(col("count") > 1)
        .count()

      println()
      println("=======================================================")
      println("TASK 2-2 DATA QUALITY CHECK")
      println("=======================================================")
      println(s"Parsed CSV columns      = ${raw.columns.length}")
      println(s"Prepared order rows     = $preparedRowCount")
      println(s"Duplicate row_id count  = $duplicateRowIdCount")
      println("=======================================================")

      require(
        duplicateRowIdCount == 0,
        "Column 'index' is not unique and cannot safely identify source rows."
      )

      val groupSizes = prepared
        .groupBy("sku", "month")
        .agg(count(lit(1)).as("group_order_count"))
        .persist(StorageLevel.MEMORY_AND_DISK)

      val skuMonthGroupCount = groupSizes.count()

      val maxGroupSize = groupSizes
        .agg(max(col("group_order_count")).as("max_group_size"))
        .first()
        .getAs[Long]("max_group_size")

      val groupsOver1000 = groupSizes
        .filter(col("group_order_count") > 1000)
        .count()

      val promotionRange = prepared
        .agg(
          min(col("promotion_count")).as("min_promotion_count"),
          max(col("promotion_count")).as("max_promotion_count")
        )
        .first()

      val minPromotionCount = promotionRange.getAs[Int]("min_promotion_count")
      val maxPromotionCount = promotionRange.getAs[Int]("max_promotion_count")

      println()
      println("=======================================================")
      println("TASK 2-2 REFERENCE SANITY CHECK")
      println("=======================================================")
      println(s"SKU-month groups            = $skuMonthGroupCount")
      println(s"Minimum promotion count     = $minPromotionCount")
      println(s"Maximum promotion count     = $maxPromotionCount")
      println(s"Largest SKU-month group     = $maxGroupSize")
      println(s"Groups with > 1000 orders   = $groupsOver1000")
      println("=======================================================")

      println()
      println("=======================================================")
      println("TASK 2-2 BENCHMARK WARM-UP")
      println("=======================================================")
      forceFullEvaluation(buildApproxSummary(prepared))
      forceFullEvaluation(buildExactSummary(prepared))
      println("Warm-up completed for APPROX and EXACT.")
      println("=======================================================")

      val approxTimes = benchmark("APPROX", BenchmarkRuns) {
        buildApproxSummary(prepared)
      }

      val exactTimes = benchmark("EXACT", BenchmarkRuns) {
        buildExactSummary(prepared)
      }

      printBenchmarkSummary("APPROX", approxTimes)
      printBenchmarkSummary("EXACT", exactTimes)

      val approxFinal = buildApproxSummary(prepared)
        .persist(StorageLevel.MEMORY_AND_DISK)

      val exactFinal = buildExactSummary(prepared)
        .persist(StorageLevel.MEMORY_AND_DISK)

      approxFinal.collect()
      exactFinal.collect()

      val a = approxFinal.alias("a")
      val e = exactFinal.alias("e")

      val combined = a
        .join(e, Seq("sku", "month"), "inner")
        .select(
          col("sku"),
          col("month"),
          a("group_order_count").as("group_order_count"),

          a("p90_approx_threshold"),
          e("p90_exact_threshold"),
          a("p90_approx_qualifying_count"),
          e("p90_exact_qualifying_count"),
          a("p90_approx_stddev"),
          e("p90_exact_stddev"),

          a("p80_approx_threshold"),
          e("p80_exact_threshold"),
          a("p80_approx_qualifying_count"),
          e("p80_exact_qualifying_count"),
          a("p80_approx_stddev"),
          e("p80_exact_stddev")
        )
        .withColumn(
          "p90_threshold_abs_diff",
          abs(col("p90_exact_threshold") - col("p90_approx_threshold"))
        )
        .withColumn(
          "p80_threshold_abs_diff",
          abs(col("p80_exact_threshold") - col("p80_approx_threshold"))
        )
        .withColumn(
          "p90_qualifying_set_diff_count",
          abs(
            col("p90_exact_qualifying_count") -
              col("p90_approx_qualifying_count")
          )
        )
        .withColumn(
          "p80_qualifying_set_diff_count",
          abs(
            col("p80_exact_qualifying_count") -
              col("p80_approx_qualifying_count")
          )
        )
        .withColumn(
          "p90_stddev_abs_diff",
          abs(col("p90_exact_stddev") - col("p90_approx_stddev"))
        )
        .withColumn(
          "p80_stddev_abs_diff",
          abs(col("p80_exact_stddev") - col("p80_approx_stddev"))
        )
        .persist(StorageLevel.MEMORY_AND_DISK)

      val resultRowCount = combined.count()

      val comparison = combined
        .agg(
          sum(
            when(col("p90_threshold_abs_diff") > CompareTolerance, 1L)
              .otherwise(0L)
          ).as("p90_threshold_diff_groups"),
          sum(
            when(col("p80_threshold_abs_diff") > CompareTolerance, 1L)
              .otherwise(0L)
          ).as("p80_threshold_diff_groups"),
          sum(
            when(col("p90_qualifying_set_diff_count") > 0L, 1L)
              .otherwise(0L)
          ).as("p90_set_diff_groups"),
          sum(
            when(col("p80_qualifying_set_diff_count") > 0L, 1L)
              .otherwise(0L)
          ).as("p80_set_diff_groups"),
          sum(
            when(col("p90_stddev_abs_diff") > CompareTolerance, 1L)
              .otherwise(0L)
          ).as("p90_stddev_diff_groups"),
          sum(
            when(col("p80_stddev_abs_diff") > CompareTolerance, 1L)
              .otherwise(0L)
          ).as("p80_stddev_diff_groups")
        )
        .first()

      val p90ThresholdDiffGroups = comparison.getAs[Long]("p90_threshold_diff_groups")
      val p80ThresholdDiffGroups = comparison.getAs[Long]("p80_threshold_diff_groups")
      val p90SetDiffGroups = comparison.getAs[Long]("p90_set_diff_groups")
      val p80SetDiffGroups = comparison.getAs[Long]("p80_set_diff_groups")
      val p90StddevDiffGroups = comparison.getAs[Long]("p90_stddev_diff_groups")
      val p80StddevDiffGroups = comparison.getAs[Long]("p80_stddev_diff_groups")

      println()
      println("=======================================================")
      println("TASK 2-2 APPROX VS EXACT COMPARISON")
      println("=======================================================")
      printDifferenceLine(
        "P90 groups with threshold difference",
        p90ThresholdDiffGroups,
        resultRowCount
      )
      printDifferenceLine(
        "P80 groups with threshold difference",
        p80ThresholdDiffGroups,
        resultRowCount
      )
      printDifferenceLine(
        "P90 groups with qualifying-set difference",
        p90SetDiffGroups,
        resultRowCount
      )
      printDifferenceLine(
        "P80 groups with qualifying-set difference",
        p80SetDiffGroups,
        resultRowCount
      )
      printDifferenceLine(
        "P90 groups with final stddev difference",
        p90StddevDiffGroups,
        resultRowCount
      )
      printDifferenceLine(
        "P80 groups with final stddev difference",
        p80StddevDiffGroups,
        resultRowCount
      )
      println("=======================================================")

      println()
      println("=======================================================")
      println("TASK 2-2 REFERENCE CASE: JNE3567-KR-L / 2022-04")
      println("=======================================================")
      combined
        .filter(
          col("sku") === "JNE3567-KR-L" &&
          col("month") === "2022-04"
        )
        .select(
          "sku",
          "month",
          "group_order_count",
          "p90_approx_threshold",
          "p90_exact_threshold",
          "p90_approx_qualifying_count",
          "p90_exact_qualifying_count",
          "p90_approx_stddev",
          "p90_exact_stddev"
        )
        .show(truncate = false)
      println("=======================================================")

      println()
      println("=======================================================")
      println("TASK 2-2 RESULT PREVIEW")
      println("=======================================================")
      combined
        .orderBy("sku", "month")
        .show(30, truncate = false)
      println(s"Task 2-2 result rows = $resultRowCount")
      println("=======================================================")

      combined
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(outputPath)

      combined.unpersist()
      approxFinal.unpersist()
      exactFinal.unpersist()
      groupSizes.unpersist()
      prepared.unpersist()

    } finally {
      spark.stop()
    }
  }

  private def buildApproxThresholds(orders: DataFrame): DataFrame = {
    orders
      .groupBy("sku", "month")
      .agg(
        percentile_approx(
          col("promotion_count"),
          lit(0.90),
          lit(ApproxAccuracy)
        ).cast("double").as("p90_approx_threshold"),
        percentile_approx(
          col("promotion_count"),
          lit(0.80),
          lit(ApproxAccuracy)
        ).cast("double").as("p80_approx_threshold")
      )
  }

  private def buildApproxSummary(orders: DataFrame): DataFrame = {
    val thresholds = buildApproxThresholds(orders)

    val attached = orders
      .join(thresholds, Seq("sku", "month"), "inner")

    attached
      .groupBy("sku", "month")
      .agg(
        count(lit(1)).as("group_order_count"),
        first(col("p90_approx_threshold"), ignoreNulls = true)
          .as("p90_approx_threshold"),
        first(col("p80_approx_threshold"), ignoreNulls = true)
          .as("p80_approx_threshold"),
        sum(
          when(
            col("promotion_count") >= col("p90_approx_threshold"),
            1L
          ).otherwise(0L)
        ).as("p90_approx_qualifying_count"),
        sum(
          when(
            col("promotion_count") >= col("p80_approx_threshold"),
            1L
          ).otherwise(0L)
        ).as("p80_approx_qualifying_count"),
        stddev_pop(
          when(
            col("promotion_count") >= col("p90_approx_threshold"),
            col("amount")
          )
        ).as("_p90_approx_stddev_raw"),
        stddev_pop(
          when(
            col("promotion_count") >= col("p80_approx_threshold"),
            col("amount")
          )
        ).as("_p80_approx_stddev_raw")
      )
      .withColumn(
        "p90_approx_stddev",
        when(
          col("p90_approx_qualifying_count") < 2L,
          lit(0.0)
        ).otherwise(
          coalesce(col("_p90_approx_stddev_raw"), lit(0.0))
        )
      )
      .withColumn(
        "p80_approx_stddev",
        when(
          col("p80_approx_qualifying_count") < 2L,
          lit(0.0)
        ).otherwise(
          coalesce(col("_p80_approx_stddev_raw"), lit(0.0))
        )
      )
      .drop("_p90_approx_stddev_raw", "_p80_approx_stddev_raw")
  }

  private def buildExactThresholds(orders: DataFrame): DataFrame = {
    val groupWindow = Window.partitionBy("sku", "month")

    val orderWindow = Window
      .partitionBy("sku", "month")
      .orderBy(
        col("promotion_count").asc,
        col("row_id").asc
      )

    val ranked = orders
      .select("row_id", "sku", "month", "promotion_count")
      .withColumn("_group_n", count(lit(1)).over(groupWindow))
      .withColumn("_rn", row_number().over(orderWindow).cast("long"))
      .withColumn(
        "_p90_position",
        (col("_group_n").cast("double") - lit(1.0)) * lit(0.90)
      )
      .withColumn(
        "_p80_position",
        (col("_group_n").cast("double") - lit(1.0)) * lit(0.80)
      )
      .withColumn(
        "_p90_lower_idx",
        floor(col("_p90_position")).cast("long") + lit(1L)
      )
      .withColumn(
        "_p90_upper_idx",
        ceil(col("_p90_position")).cast("long") + lit(1L)
      )
      .withColumn(
        "_p80_lower_idx",
        floor(col("_p80_position")).cast("long") + lit(1L)
      )
      .withColumn(
        "_p80_upper_idx",
        ceil(col("_p80_position")).cast("long") + lit(1L)
      )
      .withColumn(
        "_p90_fraction",
        col("_p90_position") - floor(col("_p90_position"))
      )
      .withColumn(
        "_p80_fraction",
        col("_p80_position") - floor(col("_p80_position"))
      )

    ranked
      .groupBy("sku", "month")
      .agg(
        first(col("_group_n"), ignoreNulls = true)
          .as("group_order_count"),
        first(col("_p90_fraction"), ignoreNulls = true)
          .as("_p90_fraction"),
        first(col("_p80_fraction"), ignoreNulls = true)
          .as("_p80_fraction"),
        max(
          when(
            col("_rn") === col("_p90_lower_idx"),
            col("promotion_count").cast("double")
          )
        ).as("_p90_lower_value"),
        max(
          when(
            col("_rn") === col("_p90_upper_idx"),
            col("promotion_count").cast("double")
          )
        ).as("_p90_upper_value"),
        max(
          when(
            col("_rn") === col("_p80_lower_idx"),
            col("promotion_count").cast("double")
          )
        ).as("_p80_lower_value"),
        max(
          when(
            col("_rn") === col("_p80_upper_idx"),
            col("promotion_count").cast("double")
          )
        ).as("_p80_upper_value")
      )
      .withColumn(
        "p90_exact_threshold",
        col("_p90_lower_value") +
          col("_p90_fraction") *
            (col("_p90_upper_value") - col("_p90_lower_value"))
      )
      .withColumn(
        "p80_exact_threshold",
        col("_p80_lower_value") +
          col("_p80_fraction") *
            (col("_p80_upper_value") - col("_p80_lower_value"))
      )
      .select(
        "sku",
        "month",
        "group_order_count",
        "p90_exact_threshold",
        "p80_exact_threshold"
      )
  }

  private def buildExactSummary(orders: DataFrame): DataFrame = {
    val thresholds = buildExactThresholds(orders)

    val attached = orders
      .join(
        thresholds.select(
          "sku",
          "month",
          "p90_exact_threshold",
          "p80_exact_threshold"
        ),
        Seq("sku", "month"),
        "inner"
      )

    attached
      .groupBy("sku", "month")
      .agg(
        count(lit(1)).as("group_order_count"),
        first(col("p90_exact_threshold"), ignoreNulls = true)
          .as("p90_exact_threshold"),
        first(col("p80_exact_threshold"), ignoreNulls = true)
          .as("p80_exact_threshold"),
        sum(
          when(
            col("promotion_count") >= col("p90_exact_threshold"),
            1L
          ).otherwise(0L)
        ).as("p90_exact_qualifying_count"),
        sum(
          when(
            col("promotion_count") >= col("p80_exact_threshold"),
            1L
          ).otherwise(0L)
        ).as("p80_exact_qualifying_count"),
        stddev_pop(
          when(
            col("promotion_count") >= col("p90_exact_threshold"),
            col("amount")
          )
        ).as("_p90_exact_stddev_raw"),
        stddev_pop(
          when(
            col("promotion_count") >= col("p80_exact_threshold"),
            col("amount")
          )
        ).as("_p80_exact_stddev_raw")
      )
      .withColumn(
        "p90_exact_stddev",
        when(
          col("p90_exact_qualifying_count") < 2L,
          lit(0.0)
        ).otherwise(
          coalesce(col("_p90_exact_stddev_raw"), lit(0.0))
        )
      )
      .withColumn(
        "p80_exact_stddev",
        when(
          col("p80_exact_qualifying_count") < 2L,
          lit(0.0)
        ).otherwise(
          coalesce(col("_p80_exact_stddev_raw"), lit(0.0))
        )
      )
      .drop("_p90_exact_stddev_raw", "_p80_exact_stddev_raw")
  }

  private def forceFullEvaluation(df: DataFrame): Int = {
    df.collect().length
  }

  private def benchmark(
      label: String,
      runs: Int
  )(
      build: => DataFrame
  ): Seq[Double] = {
    println()
    println("=======================================================")
    println(s"TASK 2-2 $label BENCHMARK")
    println("=======================================================")

    val times = (1 to runs).map { runNumber =>
      val start = System.nanoTime()
      val rows = forceFullEvaluation(build)
      val seconds = (System.nanoTime() - start).toDouble / 1e9

      println(
        f"$label%s run $runNumber%02d = $seconds%.6f seconds; result rows = $rows%d"
      )

      seconds
    }

    println("=======================================================")
    times
  }

  private def mean(values: Seq[Double]): Double = {
    values.sum / values.size.toDouble
  }

  private def populationStddev(values: Seq[Double]): Double = {
    val m = mean(values)
    math.sqrt(
      values.map(v => math.pow(v - m, 2.0)).sum / values.size.toDouble
    )
  }

  private def printBenchmarkSummary(
      label: String,
      times: Seq[Double]
  ): Unit = {
    println()
    println("=======================================================")
    println(s"TASK 2-2 $label BENCHMARK SUMMARY")
    println("=======================================================")
    println(s"Measured runs = ${times.size}")
    println(f"Mean seconds  = ${mean(times)}%.6f")
    println(f"Stddev seconds = ${populationStddev(times)}%.6f")
    println("=======================================================")
  }

  private def printDifferenceLine(
      label: String,
      countValue: Long,
      total: Long
  ): Unit = {
    val percentage =
      if (total == 0L) 0.0
      else countValue.toDouble * 100.0 / total.toDouble

    println(f"$label%-48s = $countValue%6d / $total%6d ($percentage%.1f%%)")
  }
}
