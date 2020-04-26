import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object ProfileData {
  def profileData(sc: SparkContext, mode: String, rawPath: String, cleanPath: String): Unit = {
    // Load raw and clean data
    val rawData = sc.textFile(rawPath)
    val cleanData = sc.textFile(cleanPath).map(line => line.split(','))

    // Count the number of records
    val rawCount = rawData.count
    val cleanCount = cleanData.count

    // Print counts
    println(mode + " Data\nRaw count: " + rawCount)
    println("Clean count: " + cleanCount)

    // Weather data - Count the number of records for different conditions
    if (mode == "Weather") {
      // Temperature
      var tempCount = 0L
      for (i <- 0 to 100 by 10) {
        tempCount = cleanData.map(line => ((line(4).toInt + 5) / 10) * 10).filter(line => line == i).count
        println(i + " deg. F: " + tempCount)
      }

      // Humidity
      var humidCount = 0L
      for (i <- 10 to 100 by 10) {
        humidCount = cleanData.map(line => ((line(8).toInt + 5) / 10) * 10).filter(line => line == i).count
        println(i + "% rel. humidity: " + humidCount)
      }

      // Rain
      val rainCount = cleanData.map(line => line(5)).filter(line => line == "1").count
      println("Rain: " + rainCount)

      // Snow 
      val snowCount = cleanData.map(line => line(6)).filter(line => line == "1").count
      println("Snow: " + snowCount)

      // Fog 
      val fogCount = cleanData.map(line => line(7)).filter(line => line == "1").count
      println("Fog: " + fogCount)
    }

    // Crime Data - Count the number of each reported crime type
    if (mode == "Crime"){
      val splitData = rawData.map(line => line.split(","))
      val cleanDataBefore = splitData.collect{case l if (l.length > 8) => List(l(1), l(2), l(8))}
      val filterData1 = cleanDataBefore.filter(line => line(0) != "CMPLNT_FR_DT")
      val filterData2 = filterData1.filter(line => line(2).length > 0)
      val CrimeType = filterData2.map(line => line(2)).distinct()
      val CrimeTypeSort = CrimeType.sortBy(line => line)
      val CrimeTypeArray = CrimeTypeSort.collect

      CrimeTypeArray(3) = "BURGLARY"
      CrimeTypeArray(9) = "FELONY ASSAULT"
      CrimeTypeArray(45) = "RAPE"
      CrimeTypeArray(11) = "ADMINISTRATIVE CODE"
      CrimeTypeArray(21) = "ASSAULT 3 & RELATED OFFENSES"
      CrimeTypeArray(62) = "NYS LAWS-UNCLASSIFIED VIOLATION"

      for (i <- 0 to 70){
        val CountCleanData = cleanData.filter(line => line(4) == i.toString).count
		println(i + " - " + CrimeTypeArray(i.toInt) + ": " + CountCleanData)
      }   
    }
  }

  def main(args: Array[String]) {
    val sc = new SparkContext()
  
    profileData(sc, "Weather", "/user/vag273/project/weather_data", "/user/vag273/project/clean_weather_data")
    print("\n")
    profileData(sc, "Crime", "/user/vag273/project/crime_data", "/user/vag273/project/clean_crime_data")
  }
}
