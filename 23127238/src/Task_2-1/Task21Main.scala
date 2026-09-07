package lab3.task21

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Task21Main {

  private val RequiredColumns = Seq(
    "index",
    "Date",
    "Status",
    "Fulfilment",
    "ship-service-level",
    "Courier Status",
    "Amount",
    "ship-city",
    "ship-state",
    "promotion-ids"
  )

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: Task21Main <input-csv> <output-directory>")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession.builder()
      .appName("Lab 3 Task 2-1")
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

      // ---------------------------------------------------------------------
      // 1. Prepare only the columns required by Task 2-1.
      //
      // Notes aligned with the instructor reference:
      // - Date format is MM-dd-yy.
      // - State and city are normalized with UPPER(TRIM(...)) to avoid
      //   inconsistent casing and surrounding whitespace in grouping/joining.
      // - Other categorical fields used in filtering are normalized in the
      //   same way for consistent comparisons.
      // ---------------------------------------------------------------------
      val prepared = raw.select(
        col("index").cast("long").as("row_id"),
        to_date(trim(col("Date")), "MM-dd-yy").as("order_date"),
        upper(trim(col("Status"))).as("status"),
        upper(trim(col("Fulfilment"))).as("fulfilment"),
        upper(trim(col("ship-service-level"))).as("service_level"),
        upper(trim(col("Courier Status"))).as("courier_status"),
        upper(trim(col("ship-city"))).as("city"),
        upper(trim(col("ship-state"))).as("state"),
        col("Amount").cast("double").as("amount"),
        col("promotion-ids").as("promotion_ids_raw")
      )

      // ---------------------------------------------------------------------
      // 2. Data-quality check for row_id.
      // ---------------------------------------------------------------------
      val nullRowIdCount = prepared
        .filter(col("row_id").isNull)
        .count()

      val duplicateRowIdCount = prepared
        .filter(col("row_id").isNotNull)
        .groupBy("row_id")
        .count()
        .filter(col("count") > 1)
        .count()

      println()
      println("=======================================================")
      println("TASK 2-1 DATA QUALITY CHECK")
      println("=======================================================")
      println(s"Null row_id count      = $nullRowIdCount")
      println(s"Duplicate row_id count = $duplicateRowIdCount")
      println("=======================================================")

      require(
        nullRowIdCount == 0,
        "Column 'index' contains null or non-numeric row IDs."
      )

      require(
        duplicateRowIdCount == 0,
        "Column 'index' is not unique and cannot safely be used as row_id."
      )

      // ---------------------------------------------------------------------
      // 3. Block A - Temporally valid promotions.
      //
      // A promotion is valid when:
      //   last_appearance_date - first_appearance_date >= 2 days
      //
      // All promotions are retained, including Amazon-issued promotions.
      // ---------------------------------------------------------------------
      val explodedPromos = prepared
        .filter(col("order_date").isNotNull)
        .withColumn(
          "promo",
          explode_outer(
            split(
              coalesce(col("promotion_ids_raw"), lit("")),
              ","
            )
          )
        )
        .withColumn("promo_clean", trim(col("promo")))
        .filter(length(col("promo_clean")) > 0)

      val validPromotions = explodedPromos
        .groupBy("promo_clean")
        .agg(
          min(col("order_date")).as("first_appearance_date"),
          max(col("order_date")).as("last_appearance_date")
        )
        .withColumn(
          "active_period_days",
          datediff(
            col("last_appearance_date"),
            col("first_appearance_date")
          )
        )
        .filter(col("active_period_days") >= 2)
        .select("promo_clean")

      // ---------------------------------------------------------------------
      // 4. Count valid promotion identifiers for each source order record.
      //
      // IMPORTANT:
      // Use count(), not countDistinct().
      // The instructor reference describes this as one groupBy(row/order)
      // shuffle. countDistinct() introduces an additional distinct shuffle
      // phase and therefore changes the required physical-plan analysis.
      // ---------------------------------------------------------------------
      val validPromoCounts = explodedPromos
        .join(validPromotions, Seq("promo_clean"), "inner")
        .groupBy("row_id")
        .agg(
          count(col("promo_clean")).as("valid_promotion_count")
        )

      val ordersWithPromoCount = prepared
        .join(validPromoCounts, Seq("row_id"), "left")
        .withColumn(
          "valid_promotion_count",
          coalesce(col("valid_promotion_count"), lit(0L))
        )

      // ---------------------------------------------------------------------
      // 5. Block B - State-level average Amount.
      //
      // Reference population:
      //   Fulfilment = Merchant
      //   Courier Status = Shipped
      //   grouped by state
      // ---------------------------------------------------------------------
      val stateAverages = prepared
        .filter(
          col("fulfilment") === "MERCHANT" &&
          col("courier_status") === "SHIPPED" &&
          col("state").isNotNull &&
          length(col("state")) > 0 &&
          col("amount").isNotNull
        )
        .groupBy("state")
        .agg(
          avg(col("amount")).as("state_average_amount")
        )

      val ordersWithStateAvg = ordersWithPromoCount
        .join(stateAverages, Seq("state"), "left")

      // ---------------------------------------------------------------------
      // 6. Block C - Denominator population.
      //
      // The instructor reference interprets "cancelled" as Status CONTAINS
      // "Cancelled", not only exact equality.
      //
      // Keep rows without promotions by using LEFT joins above.
      //
      // Do not drop NULL city here: Spark groupBy can retain a NULL group,
      // and keeping the source grouping behavior allows direct comparison
      // with the instructor's reported city-group count.
      // ---------------------------------------------------------------------
      val cancelledStandard = ordersWithStateAvg
        .filter(
          col("status").contains("CANCELLED") &&
          col("service_level") === "STANDARD"
        )

      val classified = cancelledStandard
        .withColumn(
          "is_qualified",
          col("valid_promotion_count") >= 3 &&
          col("amount").isNotNull &&
          col("state_average_amount").isNotNull &&
          col("amount") < col("state_average_amount")
        )

      // ---------------------------------------------------------------------
      // 7. Block D - Percentage by city.
      //
      // No orderBy() is applied to the Spark DataFrame because a global sort
      // would add a range-partition Exchange and Sort node to the physical
      // plan. The problem does not require sorted output.
      // ---------------------------------------------------------------------
      val result = classified
        .groupBy("city")
        .agg(
          count(lit(1)).as("cancelled_standard_count"),
          sum(
            when(col("is_qualified"), lit(1L))
              .otherwise(lit(0L))
          ).as("qualified_order_count")
        )
        .withColumn(
          "percentage",
          round(
            col("qualified_order_count").cast("double") * lit(100.0) /
              col("cancelled_standard_count").cast("double"),
            6
          )
        )

      // ---------------------------------------------------------------------
      // 8. Sanity checks requested/illustrated in the instructor reference.
      // These actions are intentionally executed OUTSIDE the controlled
      // stage-analysis job group, so they are not counted as stages of the
      // final Task 2-1 result action below.
      // ---------------------------------------------------------------------
      val totalPromotionCodeCount = explodedPromos
        .select("promo_clean")
        .distinct()
        .count()

      val validPromotionCodeCount = validPromotions.count()

      val cancelledStandardBase = prepared
        .filter(
          col("status").contains("CANCELLED") &&
          col("service_level") === "STANDARD"
        )

      val cancelledStandardCount = cancelledStandardBase.count()

      val cancelledStandardWithPromotionCount = cancelledStandardBase
        .filter(
          col("promotion_ids_raw").isNotNull &&
          length(trim(col("promotion_ids_raw"))) > 0
        )
        .count()

      val stateAverageCount = stateAverages.count()

      val cancelledStandardCityGroupCount = cancelledStandardBase
        .select("city")
        .distinct()
        .count()

      val qualifiedOrderCount = classified
        .filter(col("is_qualified"))
        .count()

      println()
      println("=======================================================")
      println("TASK 2-1 REFERENCE SANITY CHECK")
      println("=======================================================")
      println(s"All promotion identifiers             = $totalPromotionCodeCount")
      println(s"Temporally valid promotion identifiers = $validPromotionCodeCount")
      println(s"Cancelled + Standard rows             = $cancelledStandardCount")
      println(s"Cancelled + Standard with promotions  = $cancelledStandardWithPromotionCount")
      println(s"State-average groups                  = $stateAverageCount")
      println(s"Cancelled + Standard city groups      = $cancelledStandardCityGroupCount")
      println(s"Qualified rows                        = $qualifiedOrderCount")
      println("=======================================================")

      // Print the join-threshold configuration so the same code can be used
      // for both the default run and the no-broadcast comparison run.
      println()
      println("=======================================================")
      println("TASK 2-1 SPARK CONFIG")
      println("=======================================================")
      println(
        "spark.sql.autoBroadcastJoinThreshold = " +
          spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
      )
      println(
        "spark.sql.shuffle.partitions = " +
          spark.conf.get("spark.sql.shuffle.partitions")
      )
      println("=======================================================")

      // ---------------------------------------------------------------------
      // 9. Controlled execution for stage counting and AQE final plan.
      // ---------------------------------------------------------------------
      val stageAnalysisGroup = "TASK21_STAGE_ANALYSIS"
      val sc = spark.sparkContext

      sc.setJobGroup(
        stageAnalysisGroup,
        "Lab 3 Task 2-1 controlled execution for stage analysis"
      )

      val resultRows =
        try {
          result.collect()
        } finally {
          sc.clearJobGroup()
        }

      val statusTracker = sc.statusTracker
      var jobIds = statusTracker.getJobIdsForGroup(stageAnalysisGroup)
      var retry = 0

      while (jobIds.isEmpty && retry < 20) {
        Thread.sleep(100L)
        jobIds = statusTracker.getJobIdsForGroup(stageAnalysisGroup)
        retry += 1
      }

      val stageIds = jobIds
        .flatMap { jobId =>
          statusTracker.getJobInfo(jobId).toSeq.flatMap { jobInfo =>
            jobInfo.stageIds().toSeq
          }
        }
        .distinct
        .sorted

      println()
      println("=======================================================")
      println("TASK 2-1 RESULT")
      println("=======================================================")

      // Sort only the collected local Array for a readable preview.
      // This does NOT add a Spark Sort/Exchange node to the query plan.
      resultRows
        .sortBy(row => Option(row.getAs[String]("city")).getOrElse(""))
        .take(30)
        .foreach(println)

      println(s"Task 2-1 result rows = ${resultRows.length}")
      println("=======================================================")

      println()
      println("=======================================================")
      println("TASK 2-1 STAGE ANALYSIS")
      println("=======================================================")
      println(s"Job Group = $stageAnalysisGroup")
      println(s"Spark Job IDs = ${jobIds.sorted.mkString(", ")}")
      println(s"Number of Spark Jobs = ${jobIds.length}")
      println(s"Stage IDs = ${stageIds.mkString(", ")}")
      println(s"Number of Stages = ${stageIds.length}")
      println("=======================================================")

      println()
      println("=======================================================")
      println("=== TASK21_EXECUTION_PLAN_START ===")
      println("=======================================================")

      // Because result.collect() has already executed the query, Spark 3+/4
      // can expose the AQE final physical plan here.
      result.explain(true)

      println("=======================================================")
      println("=== TASK21_EXECUTION_PLAN_END ===")
      println("=======================================================")

      // ---------------------------------------------------------------------
      // 10. Export one Parquet part file into the supplied output directory.
      //
      // The surrounding shell workflow will move the single part-*.parquet
      // file to the final normal-filesystem filename Task_2-1.parquet.
      // Do NOT use getmerge for Parquet.
      // ---------------------------------------------------------------------
      result
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(outputPath)

    } finally {
      spark.stop()
    }
  }
}
