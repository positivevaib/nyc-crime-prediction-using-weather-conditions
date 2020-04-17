import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import spark.implicits._

object ProfileData {
  def updateCrimeStats(sqlCtx: SQLContext, df: DataFrame, row: Row, tempMat: Array[Array[Int]], humidMat: Array[Array[Int]], rainArr: Array[Int], snowArr: Array[Int], fogArr: Array[Int]): Unit = {
    val year = row.getInt(0)
    val month = row.getInt(1)
    val day = row.getInt(2)
    val minutes = row.getInt(3)
    val crime = row.getInt(4)

    df.registerTempTable("dfTable") 
    sqlCtx.sql(s"""SELECT * FROM dfTable WHERE year = $year AND month = $month AND day = $day AND minutes = $minutes""").rdd.map(row => { tempMat(crime)(row.getInt(4)) += 1; humidMat(crime)(row.getInt(8)) += 1; rainArr(crime) += 1; snowArr(crime) += 1; fogArr(crime) += 1 })
  }

  def main(args: Array[String]) {
    val sc = new SparkContext()
    val sqlCtx = new SQLContext(sc)

    import sqlCtx._

    val weatherData = sc.textFile("/user/vag273/project/clean_weather_data")
    val wSplit = weatherData.map(line => line.split(','))

    val wHeader = Seq("year", "month", "day", "minutes", "temp", "rain", "snow", "fog", "humidity")
    val wDF = wSplit.map(line => (line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8))).toDF(wHeader: _*)
    val castWDF = wDF.select(wDF("year").cast(IntegerType).as("year"), wDF("month").cast(IntegerType).as("month"), wDF("day").cast(IntegerType).as("day"), wDF("minutes").cast(IntegerType).as("minutes"), wDF("temp").cast(IntegerType).as("temp"), wDF("rain"), wDF("snow"), wDF("fog"), wDF("humidity").cast(IntegerType).as("humidity"))
  
    val crimeData = sc.textFile("/user/vag273/project/clean_crime_data")
    val cSplit = crimeData.map(line => line.split(','))

    val cHeader = Seq("year", "month", "day", "minutes", "type")
    val cDF = cSplit.map(line => (line(0), line(1), line(2), line(3), line(4))).toDF(cHeader: _*)
    val castCDF = cDF.select(cDF("year").cast(IntegerType).as("year"), cDF("month").cast(IntegerType).as("month"), cDF("day").cast(IntegerType).as("day"), cDF("minutes").cast(IntegerType).as("minutes"), cDF("type").cast(IntegerType).as("type"))

    var tempMat = Array.ofDim[Int](71, 11)
    var humidMat = Array.ofDim[Int](71, 10)
    var rainArr = Array[Int](71)
    var snowArr = Array[Int](71)
    var fogArr = Array[Int](71)

    castCDF.collect.foreach(row => updateCrimeStats(sqlCtx, castWDF, row, tempMat, humidMat, rainArr, snowArr, fogArr))
  }
}
