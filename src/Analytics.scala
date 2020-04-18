import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import spark.implicits._

object ProfileData {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val sqlCtx = new SQLContext(sc)

    import sqlCtx._

    val weatherData = sc.textFile("/user/vag273/project/clean_weather_data")
    val wSplit = weatherData.map(line => line.split(','))

    val wHeader = Seq("wyear", "wmonth", "wday", "wminutes", "temp", "rain", "snow", "fog", "humidity")
    val wDF = wSplit.map(line => (line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8))).toDF(wHeader: _*)
    val castWDF = wDF.select(wDF("wyear").cast(IntegerType).as("wyear"), wDF("wmonth").cast(IntegerType).as("wmonth"), wDF("wday").cast(IntegerType).as("wday"), wDF("wminutes").cast(IntegerType).as("wminutes"), wDF("temp").cast(IntegerType).as("temp"), wDF("rain"), wDF("snow"), wDF("fog"), wDF("humidity").cast(IntegerType).as("humidity"))
  
    val crimeData = sc.textFile("/user/vag273/project/clean_crime_data")
    val cSplit = crimeData.map(line => line.split(','))

    val cHeader = Seq("cyear", "cmonth", "cday", "cminutes", "type")
    val cDF = cSplit.map(line => (line(0), line(1), line(2), line(3), line(4))).toDF(cHeader: _*)
    val castCDF = cDF.select(cDF("cyear").cast(IntegerType).as("cyear"), cDF("cmonth").cast(IntegerType).as("cmonth"), cDF("cday").cast(IntegerType).as("cday"), cDF("cminutes").cast(IntegerType).as("cminutes"), cDF("type").cast(IntegerType).as("type")).filter("cyear >= 2006").filter("cyear <= 2017")

    val joinDF = castCDF.join(castWDF, castCDF("cyear") === castWDF("wyear") && castCDF("cmonth") === castWDF("wmonth") && castCDF("cday") === castWDF("wday") && castCDF("cminutes") === castWDF("wminutes"), "inner")
  }
}
