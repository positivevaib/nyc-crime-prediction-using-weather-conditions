import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import spark.implicits._

object ProfileData {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val sqlCtx = new SQLContext(sc)

    import sqlCtx._

    val weatherData = sc.textFile("/user/vag273/project/clean_weather_data")
    val wSplit = weatherData.map(line => line.split(','))

    val wHeader = Seq("date", "time", "temp", "rain", "snow", "fog", "humidity")
    val wDF = wSplit.map(line => (line(0), line(1), line(2), line(3), line(4), line(5), line(6))).toDF(wHeader: _*)
  
    val crimeData = sc.textFile("/user/vag273/project/clean_crime_data")
    val cSplit = crimeData.map(line => line.split(','))

    val cHeader = Seq("date", "time", "type")
    val cDF = cSplit.map(line => (line(0), line(1), line(2))).toDF(cHeader: _*)
  }
}
