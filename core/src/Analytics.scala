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

    // Data preparation
    println("Preparing data")

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

    // Prediction data
    println("Preparing prediction data")

    var featStr = ""
    for (temp <- 0 to 100 by 10) {
      for (rain <- 0 to 1) {
        for (snow <- 0 to 1) {
          for (fog <- 0 to 1) {
            for (humid <- 10 to 100 by 10) {
              featStr += temp + "," + temp * temp + "," + rain + "," + snow + "," + fog + "," + humid + "," + humid * humid + "\n"
    }}}}}
    
    val featRDD = sc.parallelize(featStr.split("\n")).map(line => line.split(','))
    val featHeader = Seq("temp", "temp2", "rain", "snow", "fog", "humidity", "humidity2")
    val featDF = featRDD.map(line => (line(0), line(1), line(2), line(3), line(4), line(5), line(6))).toDF(featHeader: _*)
    val castFDF = featDF.select(featDF("temp").cast(IntegerType).as("temp"), featDF("temp2").cast(IntegerType).as("temp2"), featDF("rain").cast(IntegerType).as("rain"), featDF("snow").cast(IntegerType).as("snow"), featDF("fog").cast(IntegerType).as("fog"), featDF("humidity").cast(IntegerType).as("humidity"), featDF("humidity2").cast(IntegerType).as("humidity2"))

    // Linear regression - Overall crime
    println("Overall crime")

    var lrRDD = joinDF.rdd.map(line => (line(9) + "," + line(10) + "," + line(11) + "," + line(12) + "," + line(13), 1)).reduceByKey(_ + _).map(line => line.toString.substring(1, line.toString.length - 1).split(','))

    val lrHeader = Seq("temp", "temp2", "rain", "snow", "fog", "humidity", "humidity2", "freq")
    var lrDF = lrRDD.map(line => (line(0), line(0).toInt*line(0).toInt, line(1), line(2), line(3), line(4), line(4).toInt*line(4).toInt, line(5))).toDF(lrHeader: _*)
    var castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("temp2").cast(IntegerType).as("temp2"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("humidity2").cast(IntegerType).as("humidity2"), lrDF("freq").cast(IntegerType).as("freq"))
  
    val cols = Array("temp", "temp2", "rain", "snow", "fog", "humidity", "humidity2")
    val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
    var featuresDF = assembler.transform(castLrDF)

    val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("freq")

    var lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/project/lr_overall")

    var trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    var preds = lrModel.transform(assembler.transform(castFDF)).rdd.map(line => line(0) + "," + line(2) + "," + line(3) + "," + line(4) + "," + line(5) + "," + line(8))
    preds.saveAsTextFile("/user/vag273/project/preds_overall")

    // Linear regression - Burglary
    println("Burglary")

    lrRDD = joinDF.rdd.filter(line => line(4) == 3).map(line => (line(9) + "," + line(10) + "," + line(11) + "," + line(12) + "," + line(13), 1)).reduceByKey(_ + _).map(line => line.toString.substring(1, line.toString.length - 1).split(','))

    lrDF = lrRDD.map(line => (line(0), line(0).toInt*line(0).toInt, line(1), line(2), line(3), line(4), line(4).toInt*line(4).toInt, line(5))).toDF(lrHeader: _*)
    castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("temp2").cast(IntegerType).as("temp2"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("humidity2").cast(IntegerType).as("humidity2"), lrDF("freq").cast(IntegerType).as("freq"))
  
    featuresDF = assembler.transform(castLrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/project/lr_burglary")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    preds = lrModel.transform(assembler.transform(castFDF)).rdd.map(line => line(0) + "," + line(2) + "," + line(3) + "," + line(4) + "," + line(5) + "," + line(8))
    preds.saveAsTextFile("/user/vag273/project/preds_burglary")

    // Linear regression - Assault
    println("Assault")

    lrRDD = joinDF.rdd.filter(line => line(4) == 9).map(line => (line(9) + "," + line(10) + "," + line(11) + "," + line(12) + "," + line(13), 1)).reduceByKey(_ + _).map(line => line.toString.substring(1, line.toString.length - 1).split(','))

    lrDF = lrRDD.map(line => (line(0), line(0).toInt*line(0).toInt, line(1), line(2), line(3), line(4), line(4).toInt*line(4).toInt, line(5))).toDF(lrHeader: _*)
    castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("temp2").cast(IntegerType).as("temp2"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("humidity2").cast(IntegerType).as("humidity2"), lrDF("freq").cast(IntegerType).as("freq"))
  
    featuresDF = assembler.transform(castLrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/project/lr_assault")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    preds = lrModel.transform(assembler.transform(castFDF)).rdd.map(line => line(0) + "," + line(2) + "," + line(3) + "," + line(4) + "," + line(5) + "," + line(8))
    preds.saveAsTextFile("/user/vag273/project/preds_assault")

    // Linear regression - Rape 
    println("Rape")

    lrRDD = joinDF.rdd.filter(line => line(4) == 45).map(line => (line(9) + "," + line(10) + "," + line(11) + "," + line(12) + "," + line(13), 1)).reduceByKey(_ + _).map(line => line.toString.substring(1, line.toString.length - 1).split(','))

    lrDF = lrRDD.map(line => (line(0), line(0).toInt*line(0).toInt, line(1), line(2), line(3), line(4), line(4).toInt*line(4).toInt, line(5))).toDF(lrHeader: _*)
    castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("temp2").cast(IntegerType).as("temp2"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("humidity2").cast(IntegerType).as("humidity2"), lrDF("freq").cast(IntegerType).as("freq"))
  
    featuresDF = assembler.transform(castLrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/project/lr_rape")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    preds = lrModel.transform(assembler.transform(castFDF)).rdd.map(line => line(0) + "," + line(2) + "," + line(3) + "," + line(4) + "," + line(5) + "," + line(8))
    preds.saveAsTextFile("/user/vag273/project/preds_rape")
  }
}
