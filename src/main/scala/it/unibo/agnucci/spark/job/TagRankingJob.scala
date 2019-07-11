package it.unibo.agnucci.spark.job

import it.unibo.agnucci.spark.job.helpers.HelperMethods._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object TagRankingJob {

  private val spark = SparkSession
    .builder()
    .appName("Tag Ranking Job")
    .getOrCreate()
  private val sc = spark.sparkContext

  //launch with spark2-submit ./BigDataSpark.jar hdfs:/user/agnucci/datasets/youtubeDataset hdfs:/user/agnucci/outputSpark
  def main(args: Array[String]): Unit = {

    val rddVideosNoError = readDatasetAndFilter(args)

    val rddTagsWithTrendingTimeAverage= getAverageTrendingTimeAndVideosCount(rddVideosNoError)

    sortAndSaveToParquet(rddTagsWithTrendingTimeAverage, args)
  }

  /**
    * Reads the dataset from hdfs and then filters out videos with errors.
    * */
  private def readDatasetAndFilter(args: Array[String]): RDD[Row]= {
    /*source parquet dataset with the following fields:
    video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,
    thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description*/
    val rddVideos = spark.read.parquet(args(0)).rdd

    //filtering out videos with errors, using the video_error_or_removed field
    rddVideos.filter(row => row.getAs[String](14).toLowerCase == "false")
  }

  /**
    * Calculates for each tag the average trending time and the videos count.
    * */
  private def getAverageTrendingTimeAndVideosCount(rddVideosNoError: RDD[Row]): RDD[Row] = {
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
    rddTagsWithTrendingTimeSum
      .map(row => Row.fromSeq(Seq(row.get(0), row.getAs[Long](1) / row.getAs[Long](2), row.get(2))))
  }

  /**
    * Sorts the rdd by videos count, then saves the result in a parquet file.
    * */
  private def sortAndSaveToParquet(rddTagsWithTrendingTimeAverage: RDD[Row], args: Array[String]): Unit = {
    //sorting the results by videos count
    val sortedRdd = rddTagsWithTrendingTimeAverage.sortBy(_.getAs[Long](2), ascending = false)

    //puts output in a single partition
    val resultRdd = sortedRdd coalesce 1

    //deletes the output folder if it already exists
    deletePathIfExists(args(1), sc)

    //saving the result in a parquet file
    spark.createDataFrame(resultRdd, getOutputParquetSchema).write.parquet(args(1))
  }
}
