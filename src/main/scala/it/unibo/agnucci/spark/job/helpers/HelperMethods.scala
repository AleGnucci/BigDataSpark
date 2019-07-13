package it.unibo.agnucci.spark.job.helpers

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object HelperMethods {

  /**
    * Removes useless fields, keeping only the "tags" field.
    * */
  def keepOnlyTagsField(fields: Seq[Any]): Seq[Any] = Seq(fields(6))

  /**
    * Parses the two dates and uses them to calculate the difference between days, returning it as the number of days.
    */
  def getTrendingTimeDays(trendingDateString: String, publishTimeString: String): Long = {
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
    * Transforms every double quotation mark in a single quotation mark
    * */
  def correctTags(tags: String): String = {
    tags.replaceAll("\\|\"\"", "\\|\"").replaceAll("\"\"\\|", "\"\\|")
  }

  /**
    * Updates the provided row with the given tag and adds a column with the value 1.
    * */
  def createRowWithSingleTag(row: Row, tag: String): Row =
    Row fromSeq row.toSeq.updated(0, tag) :+ 1L

  /**
    * Aggregates the rows in the same group.
    * */
  def createAggregatedRow(rowGroup: (Any, Iterable[Row])): Row = {
    val initialAccumulator: (Any, Long, Long) = (rowGroup._1, 0L, 0L)
    val aggregatedRows = rowGroup._2.foldLeft(initialAccumulator)(createAggregatedTuple)
    Row.fromSeq(aggregatedRows.productIterator.toList)
  }

  /**
    * Accumulates the values (trending time and videos count) from the provided row into the provided accumulator
    * */
  def createAggregatedTuple(accumulator: (Any, Long, Long), rowInGroup: Row): (Any, Long, Long) =
    (accumulator._1, accumulator._2 + rowInGroup.getAs[Long](1), accumulator._3 + rowInGroup.getAs[Long](2))

  /**
    * Defines the schema of the output parquet file.
    * */
  def getOutputParquetSchema: StructType = {
    new StructType()
      .add(StructField("tag", StringType, nullable = true))
      .add(StructField("trending_time_avg_days", LongType, nullable = true))
      .add(StructField("videos_count", LongType, nullable = true))
  }

  /**
    *
    * */
  def deletePathIfExists(path: String, sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outputPath = new Path(path)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }
  }

}
