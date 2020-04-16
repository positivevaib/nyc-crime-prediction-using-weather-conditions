import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object ProfileData {
  def profileData(sc: SparkContext, mode: String, rawPath: String, cleanPath: String): Unit {
    // Load raw and clean data
    val rawData = sc.textFile(rawPath)
    val cleanData = sc.textFile(cleanPath)

    // Count the number of records
    val rawCount = rawData.count
    val cleanCount = cleanData.count

    // Print counts
    println(mode + " Data\nRaw count: " + rawCount)
    println("Clean count: " + cleanCount)
  }

  def main(args: Array[String]) {
    val sc = new SparkContext()
  
    profileData(sc, "Weather", "/user/vag273/project/weather_data", "/user/vag273/project/clean_weather_data")
    profileData(sc, "Crime", "/user/vag273/project/crime_data", "/user/vag273/project/clean_crime_data")
  }
}
