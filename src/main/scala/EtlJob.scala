import org.apache.spark.sql.{DataFrame, SparkSession}

object EtlJob {

  //this is already available in spark shell
  private val spark = SparkSession
    .builder()
    .appName("Etl Phase")
    .getOrCreate()

  val sqlContext: SparkSession = SparkSession.builder().getOrCreate()
  import sqlContext.implicits._
  import org.apache.spark.sql.types.StringType

  def main(args: Array[String]): Unit = {

    val CAdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/CAvideos.csv")
    val DEdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/DEvideos.csv")
    val FRdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/FRvideos.csv")
    val GBdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/GBvideos.csv")
    val INdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/INvideos.csv")
    val JPdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/JPvideos.csv")
    val KRdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/KRvideos.csv")
    val MXdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/MXvideos.csv")
    val RUdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/RUvideos.csv")
    val USdf = readCsv("hdfs:/user/agnucci/datasets/youtube-new/USvideos.csv")

    val bigDf1 = CAdf.union(DEdf.union(FRdf.union(GBdf.union(USdf))))
    val MXdfCorrect = correctDf(MXdf)
    val bigDf2 = INdf.union(JPdf.union(KRdf.union(MXdf.union(MXdfCorrect.union(RUdf))))) //il problema è JP con MX e anche KR con MX
    val bigDf1Correct = correctDf(bigDf1)
    val bigDf = bigDf1Correct.union(bigDf2)

    val outputDf = bigDf.coalesce(1)
    outputDf.write.parquet("hdfs:/user/agnucci/datasets/youtubeDataset")
  }

  def correctDf(df: DataFrame): DataFrame = {
    df.withColumn("comments_disabled", $"comments_disabled".cast(StringType))
      .withColumn("ratings_disabled", $"ratings_disabled".cast(StringType))
      .withColumn("video_error_or_removed", $"video_error_or_removed".cast(StringType))
  }

  def readCsv(path: String): DataFrame = {
    spark.read.option("header", "true").option("delimiter", ",").option("inferSchema", "true").csv("path")
  }

}
