package lab3.task12

import org.apache.commons.csv.{CSVFormat, CSVParser}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.Try

/** Shared parsing and normalization utilities for Task 1-2. */
object CommonUtils {

  val ExpectedColumnCount: Int = 24
  val KeySeparator: String = "\u0001"

  object Columns {
    val Date: Int = 2
    val Style: Int = 7
    val SKU: Int = 8
    val Size: Int = 10
    val State: Int = 17
  }

  private val inputDateFormatter =
    DateTimeFormatter.ofPattern("MM-dd-yy", Locale.ROOT)

  private val monthFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM", Locale.ROOT)

  /**
   * Numeric size ordering is required for the condition size >= XXL.
   * FREE is handled separately and is never treated as XXL+.
   */
  private val sizeRank: Map[String, Int] = Map(
    "XS"  -> 0,
    "S"   -> 1,
    "M"   -> 2,
    "L"   -> 3,
    "XL"  -> 4,
    "XXL" -> 5,
    "3XL" -> 6,
    "4XL" -> 7,
    "5XL" -> 8,
    "6XL" -> 9
  )

  def normalize(value: String): String =
    Option(value)
      .getOrElse("")
      .trim
      .toUpperCase(Locale.ROOT)

  def isHeader(line: String): Boolean =
    Option(line)
      .getOrElse("")
      .startsWith("index,Order ID,Date,")

  /**
   * Apache Commons CSV is used instead of String.split because
   * promotion-ids may contain commas inside quoted CSV fields.
   */
  def parseCsvLine(line: String): Option[Array[String]] = {
    var parser: CSVParser = null

    try {
      parser = CSVParser.parse(line, CSVFormat.DEFAULT)
      val iterator = parser.iterator()

      if (!iterator.hasNext) {
        None
      } else {
        val record = iterator.next()
        Some(Array.tabulate(record.size())(record.get))
      }
    } catch {
      case _: Exception => None
    } finally {
      if (parser != null) {
        parser.close()
      }
    }
  }

  def parseMonth(value: String): Option[String] =
    Option(value)
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap(text => Try(LocalDate.parse(text, inputDateFormatter)).toOption)
      .map(_.format(monthFormatter))

  def isAtLeastXXL(size: String): Boolean = {
    val normalized = normalize(size)

    normalized != "FREE" &&
    sizeRank
      .get(normalized)
      .exists(_ >= sizeRank("XXL"))
  }
}
