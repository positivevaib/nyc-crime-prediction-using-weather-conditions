import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object CleanData {
  // Function to check if requested weather type was observed
  def checkWeather(observation: String, weather: String): String = {
    var code = observation.split('|')(0)
    var numCode = "0"

    if (code.contains(weather)) {
      numCode = "1"
    }

    return numCode
  }

  // Function to clean weather data and save to HDFS
  def cleanWeatherData(sc: SparkContext, inPath: String, outPath: String): Unit = {
    val rawData = sc.textFile(inPath)
    val splitData = rawData.map(line => line.split(","))

    // Filter out non-essential columns
    val columns = splitData.first()
    val indices = (columns.indexOf("\"DATE\""), columns.indexOf("\"HourlyDryBulbTemperature\""), columns.indexOf("\"HourlyPresentWeatherType\""), columns.indexOf("\"HourlyRelativeHumidity\""))

    val filterData = splitData.map(line => (line(indices._1), line(indices._2), line(indices._3), line(indices._4))).filter(line => line._2 != "" && !line._2.contains("s")).filter(line => line._4 != "" && !line._4.contains("*"))
 
    // Split date into year, month and day columns, time into minutes and weather type data into three separate columns for rain, snow and fog with binary '1/0' values
    val header = filterData.first()
    val contentData = filterData.filter(line => line != header)

    val cleanData = contentData.map(line => (line._1.split("T")(0).split('-')(0) + "," + line._1.split("T")(0).split('-')(1) + "," + line._1.split("T")(0).split('-')(2) + "," + (((line._1.split("T")(1).split(':')(0).toInt * 60) + (line._1.split("T")(1).split(":")(1).toInt) + 30) / 60) * 60, line._2.substring(1, line._2.length - 1) + "," + checkWeather(line._3, "RA") + "," + checkWeather(line._3, "SN") + "," + checkWeather(line._3, "FG") + "," + line._4.substring(1, line._4.length - 1))).reduceByKey((v1, v2) => v1).map(line => (line._1.split(',')(0), line._1.split(',')(1), line._1.split(',')(2), line._1.split(',')(3), line._2.split(',')(0), line._2.split(',')(1), line._2.split(',')(2), line._2.split(',')(3), line._2.split(',')(4)))

    // Reformat data to remove redundant chars
    val finalData = cleanData.map(line => line.toString.substring(2, line.toString.length - 1))

    // Save final version of cleaned data as text file
    finalData.saveAsTextFile(outPath)
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
    val CrimeTypeSort = CrimeType.sortBy(line => line)
    val CrimeTypeArray = CrimeTypeSort.collect
    CrimeTypeArray(3) = "BURGLARY"
    CrimeTypeArray(9) = "FELONY ASSAULT"
    CrimeTypeArray(45) = "RAPE"
    CrimeTypeArray(11) = "ADMINISTRATIVE CODE"
    CrimeTypeArray(21) = "ASSAULT 3 & RELATED OFFENSES"
    CrimeTypeArray(62) = "NYS LAWS-UNCLASSIFIED VIOLATION"

    // Change Crime Type to Number
    val CrimeNumber = filterData2.map(line => List(line(0), line(1), CrimeTypeArray.indexOf(line(2)).toString))

    // Change Date Format
    val DateSplit = CrimeNumber.collect{case l if (l(0).split("/").length > 2) => List(l(0).split("/")(2), l(0).split("/")(0), l(0).split("/")(1), l(1), l(2))}

    // Change Time Format
    val TimeSplit = DateSplit.collect{case line if (line(3).split(":").length > 2) => List(line(0), line(1), line(2), (((line(3).split(":")(0).toInt*60 + line(3).split(":")(1).toInt) + 30) / 60) * 60, line(4))}

    // Reformat Data
    val finalData = TimeSplit.map(line => (line(0) + "," + line(1) + "," + line(2) + "," + line(3) + "," + line(4)))

    // Save final version of cleaned data as text file
    finalData.saveAsTextFile(outPath)
  }

  def main(args: Array[String]) {
    val sc = new SparkContext()

    cleanWeatherData(sc, "/user/vag273/project/weather_data", "/user/vag273/project/clean_weather_data")
    cleanCrimeData(sc, "/user/vag273/project/crime_data", "/user/vag273/project/clean_crime_data")
  }
}
