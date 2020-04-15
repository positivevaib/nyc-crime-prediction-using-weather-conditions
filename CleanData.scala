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
    val rawData = sc.textFile(inPath)
    val splitData = rawData.map(line => line.split(","))

    // Filter out non-essential columns
    val columns = splitData.first()
    val indices = (columns.indexOf("\"DATE\""), columns.indexOf("\"HourlyDryBulbTemperature\""), columns.indexOf("\"HourlyPresentWeatherType\""), columns.indexOf("\"HourlyRelativeHumidity\""))

    val filterData = splitData.map(line => (line(indices._1), line(indices._2), line(indices._3), line(indices._4))).filter(line => line._2 != "" && !line._2.contains("s")).filter(line => line._4 != "" && !line._4.contains("*"))
 
    // Split weather type data into three separate columns for rain, snow and fog with binary 'yes/no' values
    val header = filterData.first()
    val contentData = filterData.filter(line => line != header)

    val cleanData = contentData.map(line => (line._1.split("T")(0), line._1.split("T")(1), line._2, checkWeather(line._3, "RA"), checkWeather(line._3, "SN"), checkWeather(line._3, "FG"), line._4))

    // Reformat data to remove redundant chars and to add back updated header
    var finalData = cleanData.map(line => (line._1.substring(1, 11), line._2.substring(0, 8), line._3.substring(1, line._3.length - 1), line._4, line._5, line._6, line._7.substring(1, line._7.length - 1)))

    val newHeader = Array(("date", "time", "hourly  dry  bulb temperature", "rain", "snow", "fog", "hourly relative humidity"))
    finalData = sc.parallelize(newHeader) ++ finalData 

    val trimFinalData = finalData.map(line => line.toString.substring(1, line.toString.length - 1))

    // Save final version of cleaned data as text file
    trimFinalData.saveAsTextFile(outPath)
  }

  def main(args: Array[String]) {
    val sc = new SparkContext()

    cleanWeatherData(sc, "/user/vag273/project/weather_data.csv", "/user/vag273/project/weather_data")
  }
}
