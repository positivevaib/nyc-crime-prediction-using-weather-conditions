import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object CleanData {
  // Function to check if requested weather type was observed
  def checkWeather(observation: String, weather: String): String = {
    var code = observation.split('|')(0)

    if (code.contains(weather)) {
      code = "yes"
    }
    else {
      code = "no"
    }

    return code
  }

  // Function to clean weather data and save to HDFS
  def cleanWeatherData(sc: SparkContext, inPath: String, outPath: String): Unit = {
    var data = sc.textFile(inPath)
    data = data.map(line => line.split(","))

    // Filter out non-essential columns
    val columns = data.first()
    val indices = (columns.indexOf("\"DATE\""), columns.indexOf("\"HourlyDryBulbTemperature\""), columns.indexOf("\"HourlyPresentWeatherType\""), columns.indexOf("\"HourlyRelativeHumidity\""))

    data = data.map(line => (line(indices._1), line(indices._2), line(indices._3), line(indices._4))).filter(line => line._2 != "" && !line._2.contains("s")).filter(line => line._4 != "" && !line._4.contains("*"))
 
    // Split weather type data into three separate columns for rain, snow and fog with binary 'yes/no' values
    var header = data.first()
    data = data.filter(line => line != header)

    data = data.map(line => (line._1.split("T")(0), line._1.split("T")(1), line._2, checkWeather(line._3, "RA"), checkWeather(line._3, "SN"), checkWeather(line._3, "FG"), line._4))

    // Reformat data to remove redundant chars and to add back updated header
    data = data.map(line => (line._1.substring(1, 11), line._2.substring(0, 8), line._3.substring(1, line._3.length - 1), line._4, line._5, line._6, line._7.substring(1, line._7.length - 1)))

    header = Array(("date", "time", "hourly  dry  bulb temperature", "rain", "snow", "fog", "hourly relative humidity"))
    data = sc.parallelize(header) ++ data 

    data = data.map(line => line.toString.substring(1, line.length - 1))

    // Save final version of cleaned data as text file
    trimFinalData.saveAsTextFile(outPath)
  }

  def main(args: Array[String]) {
    val sc = new SparkContext()

    cleanWeatherData(sc, "/user/vag273/project/weather_data.csv", "/user/vag273/project/weather_data")
  }
}
