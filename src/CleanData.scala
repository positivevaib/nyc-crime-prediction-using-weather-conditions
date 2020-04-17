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

    // Reformat data to remove redundant chars
    var finalData = cleanData.map(line => (line._1.substring(1, 11), line._2.substring(0, 8), line._3.substring(1, line._3.length - 1), line._4, line._5, line._6, line._7.substring(1, line._7.length - 1)))

    val trimFinalData = finalData.map(line => line.toString.substring(1, line.toString.length - 1))

    // Save final version of cleaned data as text file
    trimFinalData.saveAsTextFile(outPath)
  }

  // Function to clean crime data and save to HDFS
  def cleanCrimeData(sc: SparkContext, inPath: String, outPath: String): Unit = {
    // Import RDD Data
    val rawData = sc.textFile(inPath)

    // Split the CSV data
    val splitData = rawData.map(line => line.split(","))

    // Get Data, Time, Crime_Type from Origin Data
    val cleanData = splitData.collect{case l if (l.length > 8) => List(l(1), l(2), l(8))}

    // Get rid of first line of Data
    val filterData1 = cleanData.filter(line => line(0) != "CMPLNT_FR_DT")
    val filterData2 = filterData1.filter(line => line(2).length > 0)

    // Get all the Crime Type
    val CrimeType = filterData2.map(line => line(2)).distinct()
    val CrimeTypeArray = CrimeType.collect

    // Change Crime Type to Number
    val CrimeNumber = filterData2.map(line => List(line(0), line(1), CrimeTypeArray.indexOf(line(2))))

    // Reformat Data
    val finalData = CrimeNumber.map(line => (line(0) + "," + line(1) + "," + line(2)))

    // Save final version of cleaned data as text file
    finalData.saveAsTextFile(outPath)
  }

  def main(args: Array[String]) {
    val sc = new SparkContext()

    cleanWeatherData(sc, "/user/vag273/project/weather_data", "/user/vag273/project/clean_weather_data")
    cleanCrimeData(sc, "/user/vag273/project/crime_data", "/user/vag273/project/clean_crime_data")
  }
}
