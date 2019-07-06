import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{Row, SparkSession}

object TagRankingJob {

  private val spark = SparkSession
    .builder()
    .appName("Tag Ranking Job")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    /*source dataset with the following fields:
    video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,
    thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description*/
    val rddVideos = spark.read.parquet("hdfs:/user/agnucci/datasets/youtubeDataset").rdd

    //filtering out videos with errors, using the video_error_or_removed field
    val rddVideosNoError = rddVideos.filter(row => row.getAs[String](14).toLowerCase == "false")

    //removing useless fields and calculating trending time for each row
    //fields in this rdd: tags, trendingTime
    val rddVideosWithTrendingTime = rddVideosNoError
      .map(row => Row.fromSeq(keepOnlyTagsField(row.toSeq) :+
        getTrendingTimeDays(row.getAs[String](1), row.getAs[String](5))))

    //makes every double quotation mark a single quotation mark
    //fields in this rdd: tags, trendingTime
    val rddVideosWithCorrectTags = rddVideosWithTrendingTime
      .map(row => Row.fromSeq(Seq(correctTags(row.getAs[String](0)), row.get(1))))

    //creating for each row as many new rows as the amount of tags for that initial row
    //fields in this rdd: tag, trendingTime, videosCount (this last one always has value 1)
    val rddTags = rddVideosWithCorrectTags
      .flatMap(row => row.getAs[String](0).split("\\|").map(tag => createRowWithSingleTag(row, tag)))

    /*grouping the rows by tag, then aggregating the groups to calculate for each tag the videos count and the sum
    of trending times (to be later used to calculate the mean trending time)*/
    val rddTagsWithTrendingTimeSum = rddTags.groupBy(row => row.get(0)).map(rowGroup => createAggregatedRow(rowGroup))

    //calculating the mean trending time
    //fields in this rdd: tag, meanTrendingTime, videosCount
    val rddTagsWithTrendingTimeAverage = rddTagsWithTrendingTimeSum
      .map(row => Row.fromSeq(Seq(row.get(0), row.getAs[Long](1)/row.getAs[Long](2), row.get(2))))

    //sorting the results by videos count
    val sortedRdd = rddTagsWithTrendingTimeAverage.sortBy(_.getAs[Long](2), ascending = false)

    println("[tag, mean trending time, videos count]:")
    sortedRdd take 100 foreach println

    //saving the result in a file
    //sortedRdd coalesce 1 saveAsTextFile "hdfs:/user/agnucci/outputSpark"
  }

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
    tags.replaceAll("|\"\"", "|\"").replaceAll("\"\"|", "\"|")
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

}
