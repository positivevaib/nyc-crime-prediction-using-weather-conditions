import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType

object Analytics {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val sqlCtx = new SQLContext(sc)

    import sqlCtx._
    import sqlCtx.implicits._

    val weatherData = sc.textFile("/user/vag273/project/clean_weather_data")
    val wSplit = weatherData.map(line => line.split(','))

    val wHeader = Seq("wyear", "wmonth", "wday", "wminutes", "temp", "rain", "snow", "fog", "humidity")
    val wDF = wSplit.map(line => (line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8))).toDF(wHeader: _*)
    val castWDF = wDF.select(wDF("wyear").cast(IntegerType).as("wyear"), wDF("wmonth").cast(IntegerType).as("wmonth"), wDF("wday").cast(IntegerType).as("wday"), wDF("wminutes").cast(IntegerType).as("wminutes"), wDF("temp").cast(IntegerType).as("temp"), wDF("rain").cast(IntegerType).as("rain"), wDF("snow").cast(IntegerType).as("snow"), wDF("fog").cast(IntegerType).as("fog"), wDF("humidity").cast(IntegerType).as("humidity"))
  
    val crimeData = sc.textFile("/user/vag273/project/clean_crime_data")
    val cSplit = crimeData.map(line => line.split(','))

    val cHeader = Seq("cyear", "cmonth", "cday", "cminutes", "type")
    val cDF = cSplit.map(line => (line(0), line(1), line(2), line(3), line(4))).toDF(cHeader: _*)
    val castCDF = cDF.select(cDF("cyear").cast(IntegerType).as("cyear"), cDF("cmonth").cast(IntegerType).as("cmonth"), cDF("cday").cast(IntegerType).as("cday"), cDF("cminutes").cast(IntegerType).as("cminutes"), cDF("type").cast(IntegerType).as("type")).filter("cyear >= 2006").filter("cyear <= 2017")

    val joinDF = castCDF.join(castWDF, castCDF("cyear") === castWDF("wyear") && castCDF("cmonth") === castWDF("wmonth") && castCDF("cday") === castWDF("wday") && castCDF("cminutes") === castWDF("wminutes"), "left").na.drop()
    val lrRDD = joinDF.rdd.map(line => (line(9) + "," + line(10) + "," + line(11) + "," + line(12) + "," + line(13), 1)).reduceByKey(_ + _).map(line => line.toString.substring(1, line.toString.length - 1).split(','))
    val lrHeader = Seq("temp", "rain", "snow", "fog", "humidity", "freq")
    val lrDF = lrRDD.map(line => (line(0), line(1), line(2), line(3), line(4), line(5))).toDF(lrHeader: _*)
    val castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("freq").cast(IntegerType).as("freq"))

    val cols = Array("temp", "rain", "snow", "fog", "humidity")
    val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
    val featuresDF = assembler.transform(castLrDF)

    val seed = 5043
    val Array(trainData, testData) = featuresDF.randomSplit(Array(0.7, 0.3), seed)
    val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("freq").setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

    val lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/project/model")

    val trainSummary = lrModel.summary
    println(trainSummary.r2)

    val predictDF = lrModel.transform(testData)
  }
}
