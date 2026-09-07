package lab3.task11

import java.util.Locale
import org.apache.commons.csv.{CSVFormat, CSVParser}
import scala.util.Try

object CommonUtils {

  val ExpectedColumnCount: Int = 24

  object Columns {
    val Date: Int = 2
    val Status: Int = 3
    val Size: Int = 10
    val Qty: Int = 13
    val Amount: Int = 15
    val State: Int = 17
  }

  def isHeader(line: String): Boolean = {
    Option(line)
      .getOrElse("")
      .startsWith("index,Order ID,Date,")
  }

  def parseCsvLine(line: String): Option[Array[String]] = {
    var parser: CSVParser = null

    try {
      parser = CSVParser.parse(line, CSVFormat.DEFAULT)
      val iterator = parser.iterator()

      if (!iterator.hasNext) {
        None
      } else {
        val record = iterator.next()
        Some(Array.tabulate(record.size()) { index => record.get(index) })
      }
    } catch {
      case _: Exception => None
    } finally {
      if (parser != null) {
        parser.close()
      }
    }
  }

  def normalize(value: String): String = {
    Option(value)
      .getOrElse("")
      .trim
      .toUpperCase(Locale.ROOT)
  }

  def parseInt(value: String): Option[Int] = {
    Option(value)
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap(text => Try(text.toInt).toOption)
  }

  def parseDouble(value: String): Option[Double] = {
    Option(value)
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap(text => Try(text.toDouble).toOption)
  }

  /**
   * Task 1-1 interpretation chosen for the submission/reference slide:
   * an item is bought when Status contains SHIPPED and Qty > 0.
   */
  def isBought(status: String, qty: Int): Boolean = {
    normalize(status).contains("SHIPPED") && qty > 0
  }

  /**
   * Final tie-break rule required by Task 1-1: lexicographic order.
   * This is intentionally different from the numeric size ranking used by Task 1-2.
   */
  def compareSizeLexicographically(a: String, b: String): Int = {
    normalize(a).compareTo(normalize(b))
  }
}
