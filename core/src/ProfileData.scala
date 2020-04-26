import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object ProfileData {
  def profileData(sc: SparkContext, mode: String, rawPath: String, cleanPath: String): Unit = {
    // Load raw and clean data
    val rawData = sc.textFile(rawPath)
    val cleanData = sc.textFile(cleanPath)

    // Count the number of records
    val rawCount = rawData.count
    val cleanCount = cleanData.count

    // Print counts
    println(mode + " Data\nRaw count: " + rawCount)
    println("Clean count: " + cleanCount)

    // Weather data - Count the number of records for different conditions
    if (mode == "Weather") {
      // Temperature
      var tempCount = 0
      for (i <- 0 to 100 by 10) {
        tempCount = cleanData.map(line => ((line(4) + 5) / 10) * 10).filter(line => line == i).count
        println(i + " deg. F count: " + tempCount)
      }

      // Humidity
      var humidCount = 0
      for (i <- 10 to 100 by 10) {
        humidCount = cleanData.map(line => ((line(8) + 5) / 10) * 10).filter(line => line == i).count
        println(i + "% rel. humidity count: " + humidCount)
      }

      // Rain
      val rainCount = cleanData.map(line => line(5)).filter(line => line == 1).count
      println("Rain count: " + rainCount)

      // Snow 
      val snowCount = cleanData.map(line => line(6)).filter(line => line == 1).count
      println("Snow count: " + snowCount)

      // Fog 
      val fogCount = cleanData.map(line => line(7)).filter(line => line == 1).count
      println("Fog count: " + fogCount)
    }
  }

  def main(args: Array[String]) {
    val sc = new SparkContext()
  
    profileData(sc, "Weather", "/user/vag273/project/weather_data", "/user/vag273/project/clean_weather_data")
    profileData(sc, "Crime", "/user/vag273/project/crime_data", "/user/vag273/project/clean_crime_data")
  }
}
