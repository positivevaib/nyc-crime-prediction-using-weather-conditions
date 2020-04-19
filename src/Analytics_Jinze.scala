import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import spark.implicits._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.RandomForestClassifier

object ProfileData {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val sqlCtx = new SQLContext(sc)

    import sqlCtx._

    val weatherData = sc.textFile("BDADProject/clean_weather_data")
    val wSplit = weatherData.map(line => line.split(',')).map(line => (line(0) + "," + line(1) + "," + line(2) + "," + line(3), line(4) + "," + line(5) + "," + line(6) + "," + line(7) + "," + line(8))).groupByKey.map(line => line._1 + "," + line._2.toList(0)).map(line => line.split(','))


    val wHeader = Seq("wyear", "wmonth", "wday", "wminutes", "temp", "rain", "snow", "fog", "humidity")
    val wDF = wSplit.map(line => (line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8))).toDF(wHeader: _*)
    val castWDF = wDF.select(wDF("wyear").cast(IntegerType).as("wyear"), wDF("wmonth").cast(IntegerType).as("wmonth"), wDF("wday").cast(IntegerType).as("wday"), wDF("wminutes").cast(IntegerType).as("wminutes"), wDF("temp").cast(IntegerType).as("temp"), wDF("rain").cast(IntegerType).as("rain"), wDF("snow").cast(IntegerType).as("snow"), wDF("fog").cast(IntegerType).as("fog"), wDF("humidity").cast(IntegerType).as("humidity"))
  
    val crimeData = sc.textFile("BDADProject/clean_crime_data")
    val cSplit = crimeData.map(line => line.split(','))

    val cHeader = Seq("cyear", "cmonth", "cday", "cminutes", "type")
    val cDF = cSplit.map(line => (line(0), line(1), line(2), line(3), line(4))).toDF(cHeader: _*)
    val castCDF = cDF.select(cDF("cyear").cast(IntegerType).as("cyear"), cDF("cmonth").cast(IntegerType).as("cmonth"), cDF("cday").cast(IntegerType).as("cday"), cDF("cminutes").cast(IntegerType).as("cminutes"), cDF("type").cast(IntegerType).as("type")).filter("cyear >= 2006").filter("cyear <= 2017")

    val joinDF = castCDF.join(castWDF, castCDF("cyear") === castWDF("wyear") && castCDF("cmonth") === castWDF("wmonth") && castCDF("cday") === castWDF("wday") && castCDF("cminutes") === castWDF("wminutes"), "left").na.drop()

    // Change DataFrame to RDD
    val joinRDD = joinDF.rdd

    // Find the percentage for all weather
    val weatherSplit = weatherData.map(line => line.split(","))
    val allweather  = weatherSplit.map(line => line(4)+ "," + line(5)+ "," +  line(6)+ "," + line(7) + "," + line(8))
    val allweatherTuple = allweather.map(line => (line, 1))
    val allweatherSum = allweatherTuple.reduceByKey(_ + _)
    val Sum = allweatherSum.map(_._2).reduce(_ + _)
    val sortedallweatherSum = allweatherSum.sortBy(_._2)
    val allweatherpercent = allweatherSum.map(line => (line._1, line._2.toDouble / Sum.toDouble))
    val sortedallweatherpercent = allweatherpercent.sortBy(_._2)
    val sortedallweathertypes = sortedallweatherpercent.map(line => line._1).collect
    val sortedallweatherhappens =  sortedallweatherpercent.map(line => line._2).collect

    // Find the comparable crime happens for different temp
    val joinallweather = joinRDD.map(line => ( line(9)+ "," + line(10)+ "," +  line(11)+ "," + line(12) + "," + line(13), 1))
    val reducejoinallweather = joinallweather.reduceByKey(_ + _)
    val realjoinallweather = reducejoinallweather.map(line => (line._1, (line._2.toDouble / sortedallweatherhappens(sortedallweathertypes.indexOf(line._1))).toInt))

    // Rank all weather based on comparable Crime happens
    val rankjoinallweather = realjoinallweather.collect.toList.sortBy(_._2).reverse

    // Find which crime happens most in exactly one weather type
    val joincrimeweather = joinRDD.map(line => ( line(4) + "," + line(9)+ "," + line(10)+ "," +  line(11)+ "," + line(12) + "," + line(13), 1))
    val reducejoincrimeweather = joincrimeweather.reduceByKey(_ + _)
    val reformatcrimeweather = reducejoincrimeweather.map(line => (line._1.split(",")(1) + "," + line._1.split(",")(2) + "," + line._1.split(",")(3) + "," +line._1.split(",")(4) + "," + line._1.split(",")(5) + "," + line._2, line._1.split(",")(0)))
    val rankcrimeweather = reformatcrimeweather.collect.toList.sortBy(_._1).reverse
  }
}
