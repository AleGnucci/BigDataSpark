package it.unibo.agnucci.spark.job.helpers

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object HelperMethods {

  /**
    * Parses the two dates and uses them to calculate the difference between days, returning it as the number of days.
    */
  def getTrendingTimeDays(publishTimeString: String, trendingDateString: String): Long = {
    val publishTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").parse(publishTimeString)
    val trendingDate = new SimpleDateFormat("yy.dd.MM").parse(trendingDateString)
    dateDaysDifference(publishTime, trendingDate)
  }

  /**
    * Calculates the difference between days, returning it as the number of days.
    * */
  def dateDaysDifference(beforeDate: Date, afterDate: Date): Long =
    TimeUnit.DAYS.convert(Math.abs(afterDate.getTime - beforeDate.getTime), TimeUnit.MILLISECONDS)

  /**
    * Removes the quotation marks from the tags string. It is the same as this:
    * tags.toLowerCase.replaceAll("\\|\"\"\"", "\\|").replaceAll("\"\"\"\\|", "\\|")
    * .replaceAll("\\|\"\"", "\\|").replaceAll("\"\"\\|", "\\|")
    * .replaceAll("\\|\"", "\\|").replaceAll("\"\\|", "\\|")
    * */
  def correctTags(tags: String): String =
    tags.toLowerCase.replaceAll("[\\|\"\"|\"\"\\||\\|\"|\"\\||\\|\"\"\"|\"\"\"\\|]", "\\|")

  /**
    * Defines the schema of the output parquet file.
    * */
  def getOutputParquetSchema: StructType =
    new StructType()
      .add(StructField("tag", StringType, nullable = true))
      .add(StructField("trending_time_avg_days", LongType, nullable = true))
      .add(StructField("videos_count", LongType, nullable = true))

}
