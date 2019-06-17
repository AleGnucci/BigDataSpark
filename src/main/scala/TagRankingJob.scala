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

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val rddVideos = spark.read.parquet("people.parquet").rdd
    val rddVideosNoError = rddVideos.filter(row => row.get(14) == "False")
    val rddVideosWithTrendingTime = rddVideosNoError
      .map(row => Row.fromSeq(row.toSeq :+ getTrendingTime(row.getAs[String](1), row.getAs[String](5))))
    val rddTags = rddVideosWithTrendingTime
      .flatMap(row => row.getAs[String](6).split("|").map(tag => createRowWithSingleTag(row, tag)))
    val rddVideosWithAverageTrendingTime = rddVideosWithTrendingTime.groupBy(row => row.get(6)).map(row => (row._1, row._2.aggregate()))
  }

  private def getTrendingTime(trendingDateString: String, publishTimeString: String): Long = {
    val publishTime = new SimpleDateFormat("yyyyy-MM-ddTHH:mm:ss.SSSz").parse(publishTimeString)
    val trendingDate = new SimpleDateFormat("yy.dd.MM").parse(trendingDateString)
    dateDaysDifference(publishTime, trendingDate)
  }

  private def dateDaysDifference(beforeDate: Date, afterDate: Date): Long = {
    TimeUnit.DAYS.convert(Math.abs(afterDate.getTime - beforeDate.getTime), TimeUnit.MILLISECONDS)
  }

  private def createRowWithSingleTag(row: Row, tag: String): Row = {
    Row fromSeq row.toSeq.updated(6, tag) :+ 1
  }

}
