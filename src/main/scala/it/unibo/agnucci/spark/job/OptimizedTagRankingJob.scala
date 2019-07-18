package it.unibo.agnucci.spark.job

import it.unibo.agnucci.spark.job.helpers.HelperMethods._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object OptimizedTagRankingJob {

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
    trending_date, publish_time, tags, video_error_or_removed*/
    val rddVideos = spark.read.parquet(args(0)).rdd

    //filtering out videos with errors, using the video_error_or_removed field
    rddVideos.filter(row => !row.getAs[Boolean](3))
  }

  /**
    * Calculates for each tag the average trending time and the videos count.
    * */
  private def getAverageTrendingTimeAndVideosCount(rddVideosNoError: RDD[Row]): RDD[(String, (Long, Long))] = {
    //removes useless fields and calculates trending time for each row
    //also it makes every double quotation mark a single quotation mark in the tags field and converts it to lowercase
    //pair rdd: the key is tags, the value is trendingTime
    val rddVideosWithTrendingTime = rddVideosNoError
      .map(row => (correctTags(row.getAs[String](2)), getTrendingTimeDays(row.getAs[String](1), row.getAs[String](0))))

    //creating for each pair as many new pairs as the amount of tags for that initial pair
    //pair rdd: the key is the tag, the value is trendingTime and videosCount (this last one always has value 1)
    val rddTags = rddVideosWithTrendingTime
      .flatMap(pair => pair._1.split("\\|").distinct.map(tag => (tag, (pair._2, 1L))))

    /*aggregates the groups to calculate for each tag the videos count and the sum
    of trending times (to be later used to calculate the mean trending time)*/
    //reduceByKey shuffles the data and sets a partitioner
    //pair rdd: the key is the tag, the value is trendingTime and videosCount
    val rddTagsWithTrendingTimeSum = rddTags
      .reduceByKey((value1, value2) => (value1._1 + value2._1, value1._2 + value2._2))

    //calculating the mean trending time
    //mapValues preserves the previous partitioner
    //pair rdd: the key is the tag, the value is meanTrendingTime and videosCount
    rddTagsWithTrendingTimeSum.mapValues(value => (value._1 / value._2, value._2))
  }

  /**
    * Sorts the rdd by videos count, then saves the result in a parquet file.
    * */
  private def sortAndSaveToParquet(notSortedRdd: RDD[(String, (Long, Long))], args: Array[String]): Unit = {
    //sorting the results by videos count
    //sortBy shuffles the data and sets a RangePartitioner
    val sortedRdd = notSortedRdd.sortBy(_._2._2, ascending = false)

    //converts the pair rdd to a row rdd
    //map removes the previous partitioner
    val rowRdd = sortedRdd.map(pair => Row.fromTuple(pair._1, pair._2._1, pair._2._2))

    //puts output in a single partition
    val resultRdd = rowRdd coalesce 1

    //deletes the output folder if it already exists
    deletePathIfExists(args(1), sc)

    //saving the result in a parquet file
    spark.createDataFrame(resultRdd, getOutputParquetSchema).write.parquet(args(1))
  }

}
