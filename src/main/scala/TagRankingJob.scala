import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{Row, SparkSession}

class TagRankingJob {

  private val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    /*source dataset with the following fields:
    video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,
    thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description*/
    val rddVideos = spark.read.parquet("/user/agnucci/datasets/youtubeDataset.parquet").rdd

    //filtering out videos with errors, using the video_error_or_removed field
    val rddVideosNoError = rddVideos.filter(_.get(14) == "False")

    //removing useless fields and calculating trending time for each row
    //fields in this rdd: tags, trendingTime
    val rddVideosWithTrendingTime = rddVideosNoError
      .map(row => Row.fromSeq(keepOnlyTagsField(row.toSeq) :+
        getTrendingTimeDays(row.getAs[String](1), row.getAs[String](5))))

    //creating for each row as many new rows as the amount of tags for that initial row
    //fields in this rdd: tag, trendingTime, videosCount (this last one always has value 1)
    val rddTags = rddVideosWithTrendingTime
      .flatMap(row => row.getAs[String](0).split("|").map(tag => createRowWithSingleTag(row, tag)))

    /*grouping the rows by tag, then aggregating the groups to calculate for each tag the videos count and the sum
    of trending times (to be later used to calculate the mean trending time)*/
    val rddTagsWithTrendingTimeSum = rddTags.groupBy(row => row.get(0)).map(rowGroup => createAggregatedRow(rowGroup))

    //calculating the mean trending time
    //fields in this rdd: tag, meanTrendingTime, videosCount
    val rddTagsWithTrendingTimeAverage = rddTagsWithTrendingTimeSum
      .map(row => Row.fromTuple((row.get(0), row.getAs[Long](1)/row.getAs[Long](2), row.get(2))))

    //sorting the results by meanTrendingTime
    val sortedRdd = rddTagsWithTrendingTimeAverage.sortBy(_.getAs[Long](2), ascending = false)

    //displaying the first 100 rows of the result
    sortedRdd take 100 foreach(println(_))
  }

  /**
    * Removes useless fields, keeping only the "tags" field.
    * */
  private def keepOnlyTagsField(fields: Seq[Any]): Seq[Any] = Seq(fields(6))

  /**
  * Parses the two dates and uses them to calculate the difference between days, returning it as the number of days.
  */
  private def getTrendingTimeDays(trendingDateString: String, publishTimeString: String): Long = {
    val publishTime = new SimpleDateFormat("yyyyy-MM-ddTHH:mm:ss.SSSz").parse(publishTimeString)
    val trendingDate = new SimpleDateFormat("yy.dd.MM").parse(trendingDateString)
    dateDaysDifference(publishTime, trendingDate)
  }

  /**
    * Calculates the difference between days, returning it as the number of days.
    * */
  private def dateDaysDifference(beforeDate: Date, afterDate: Date): Long =
    TimeUnit.DAYS.convert(Math.abs(afterDate.getTime - beforeDate.getTime), TimeUnit.MILLISECONDS)

  /**
    * Updates the provided row with the given tag and adds a column with the value 1.
    * */
  private def createRowWithSingleTag(row: Row, tag: String): Row =
    Row fromSeq row.toSeq.updated(0, tag) :+ 1

  /**
    * Aggregates the rows in the same group.
    * */
  private def createAggregatedRow(rowGroup: (Any, Iterable[Row])): Row = {
    val resultTuple = (rowGroup._1, rowGroup._2.aggregate((rowGroup._1, 0, 0)
      ((accumulator: (Any, Long, Long), rowInGroup: Row) => createAggregatedTuple(accumulator, rowInGroup))))
    Row.fromTuple(resultTuple)
  }

  /**
    * Accumulates the values (trending time and videos count) from the provided row into the provided accumulator
    * */
  private def createAggregatedTuple(accumulator: (Any, Long, Long), rowInGroup: Row): (Any, Long, Long) =
    (accumulator._1, accumulator._2 + rowInGroup.getAs[Long](1), accumulator._3 + rowInGroup.getAs[Long](2))


}
