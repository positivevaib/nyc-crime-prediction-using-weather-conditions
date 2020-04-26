import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.mllib.evaluation.RegressionMetrics

val sqlCtx = new SQLContext(sc)
import sqlCtx._
import sqlCtx.implicits._
val weatherData = sc.textFile("/user/vag273/project/clean_weather_data")
val wSplit = weatherData.map(line => line.split(',')).map(line => (line(0) + "," + line(1) + "," + line(2) + "," + line(3), line(4) + "," + line(5) + "," + line(6) + "," + line(7) + "," + line(8))).groupByKey.map(line => line._1 + "," + line._2.toList(0)).map(line => line.split(','))
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

val Crime3rdd = joinDF.rdd.filter(line => line(4) == 3).map(line => (line(9) + "," + line(10) + "," + line(11) + "," + line(12) + "," + line(13), 1)).reduceByKey(_ + _).map(line => line.toString.substring(1, line.toString.length - 1).split(','))

val Crime9rdd = joinDF.rdd.filter(line => line(4) == 9).map(line => (line(9) + "," + line(10) + "," + line(11) + "," + line(12) + "," + line(13), 1)).reduceByKey(_ + _).map(line => line.toString.substring(1, line.toString.length - 1).split(','))

val Crime45rdd = joinDF.rdd.filter(line => line(4) == 45).map(line => (line(9) + "," + line(10) + "," + line(11) + "," + line(12) + "," + line(13), 1)).reduceByKey(_ + _).map(line => line.toString.substring(1, line.toString.length - 1).split(','))

// For Linear regression - All Crime type - 0.555040806189814
val lrHeader = Seq("temp", "temp2", "rain", "snow", "fog", "humidity", "humidity2", "freq")
val lrDF = lrRDD.map(line => (line(0), line(0).toInt*line(0).toInt, line(1), line(2), line(3), line(4), line(4).toInt*line(4).toInt, line(5))).toDF(lrHeader: _*)
val castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("temp2").cast(IntegerType).as("temp2"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("humidity2").cast(IntegerType).as("humidity2"), lrDF("freq").cast(IntegerType).as("freq"))
val cols = Array("temp", "temp2", "rain", "snow", "fog", "humidity", "humidity2")

val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featuresDF = assembler.transform(castLrDF)
val seed = 5043
val Array(trainData, testData) = featuresDF.randomSplit(Array(0.7, 0.3), seed)
val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("freq")
val lrModel = lr.fit(featuresDF)
lrModel.write.overwrite().save("/user/vag273/project/LR_OverallMOD")
val trainSummary = lrModel.summary
println(trainSummary.r2)

// For Linear regression - crime type 3 - 0.5598261147376721
val lrHeader = Seq("temp", "temp2", "rain", "snow", "fog", "humidity", "humidity2", "freq")
val lrDF = Crime3rdd.map(line => (line(0), line(0).toInt*line(0).toInt, line(1), line(2), line(3), line(4), line(4).toInt*line(4).toInt, line(5))).toDF(lrHeader: _*)
val castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("temp2").cast(IntegerType).as("temp2"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("humidity2").cast(IntegerType).as("humidity2"), lrDF("freq").cast(IntegerType).as("freq"))
val cols = Array("temp", "temp2", "rain", "snow", "fog", "humidity", "humidity2")

val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featuresDF = assembler.transform(castLrDF)
val seed = 5043
val Array(trainData, testData) = featuresDF.randomSplit(Array(0.7, 0.3), seed)
val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("freq")
val lrModel = lr.fit(featuresDF)
lrModel.write.overwrite().save("/user/vag273/project/LR_3MOD")
val trainSummary = lrModel.summary
println(trainSummary.r2)

// For Linear regression - crime type 9 - 0.5539486956808137
val lrHeader = Seq("temp", "temp2", "rain", "snow", "fog", "humidity", "humidity2", "freq")
val lrDF = Crime9rdd.map(line => (line(0), line(0).toInt*line(0).toInt, line(1), line(2), line(3), line(4), line(4).toInt*line(4).toInt, line(5))).toDF(lrHeader: _*)
val castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("temp2").cast(IntegerType).as("temp2"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("humidity2").cast(IntegerType).as("humidity2"), lrDF("freq").cast(IntegerType).as("freq"))
val cols = Array("temp", "temp2", "rain", "snow", "fog", "humidity", "humidity2")

val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featuresDF = assembler.transform(castLrDF)
val seed = 5043
val Array(trainData, testData) = featuresDF.randomSplit(Array(0.7, 0.3), seed)
val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("freq")
val lrModel = lr.fit(featuresDF)
lrModel.write.overwrite().save("/user/vag273/project/LR_9MOD")
val trainSummary = lrModel.summary
println(trainSummary.r2)

// For Linear regression - crime type 45 - 0.5550674958835622
val lrHeader = Seq("temp", "temp2", "rain", "snow", "fog", "humidity", "humidity2", "freq")
val lrDF = Crime45rdd.map(line => (line(0), line(0).toInt*line(0).toInt, line(1), line(2), line(3), line(4), line(4).toInt*line(4).toInt, line(5))).toDF(lrHeader: _*)
val castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("temp2").cast(IntegerType).as("temp2"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("humidity2").cast(IntegerType).as("humidity2"), lrDF("freq").cast(IntegerType).as("freq"))
val cols = Array("temp", "temp2", "rain", "snow", "fog", "humidity", "humidity2")

val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featuresDF = assembler.transform(castLrDF)
val seed = 5043
val Array(trainData, testData) = featuresDF.randomSplit(Array(0.7, 0.3), seed)
val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("freq")
val lrModel = lr.fit(featuresDF)
lrModel.write.overwrite().save("/user/vag273/project/LR_45MOD")
val trainSummary = lrModel.summary
println(trainSummary.r2)

// For Random forest - All crime type, 5 features - 0.6143350311121288
val lrHeader = Seq("temp", "rain", "snow", "fog", "humidity", "freq")
val lrDF = lrRDD.map(line => (line(0),  line(1), line(2), line(3), line(4), line(5))).toDF(lrHeader: _*)
val castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("freq").cast(IntegerType).as("freq"))
val cols = Array("temp", "rain", "snow", "fog", "humidity")

val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featuresDF = assembler.transform(castLrDF)
val seed = 5043
val Array(trainData, testData) = featuresDF.randomSplit(Array(0.7, 0.3), seed)
val rf = new RandomForestRegressor().setSeed(seed).setLabelCol("freq").setFeaturesCol("features").setNumTrees(100).setMaxDepth(10)

val rfModel = rf.fit(featuresDF)
rfModel.write.overwrite().save("/user/vag273/project/RF_OverallMOD")

val predictDF = rfModel.transform(testData)
val TruePredict = predictDF.rdd.map(line => (line(5).toString.toDouble, line(7).toString.toDouble))
val rm = new RegressionMetrics(TruePredict)

println(rm.r2)

// For Random forest - crime type 3, 5 features - 0.5290029172343879
val lrHeader = Seq("temp", "rain", "snow", "fog", "humidity", "freq")
val lrDF = Crime3rdd.map(line => (line(0),  line(1), line(2), line(3), line(4), line(5))).toDF(lrHeader: _*)
val castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("freq").cast(IntegerType).as("freq"))
val cols = Array("temp", "rain", "snow", "fog", "humidity")

val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featuresDF = assembler.transform(castLrDF)
val seed = 5043
val Array(trainData, testData) = featuresDF.randomSplit(Array(0.7, 0.3), seed)
val rf = new RandomForestRegressor().setSeed(seed).setLabelCol("freq").setFeaturesCol("features").setNumTrees(100).setMaxDepth(10)

val rfModel = rf.fit(featuresDF)
rfModel.write.overwrite().save("/user/vag273/project/RF_3MOD")

val predictDF = rfModel.transform(testData)
val TruePredict = predictDF.rdd.map(line => (line(5).toString.toDouble, line(7).toString.toDouble))
val rm = new RegressionMetrics(TruePredict)

println(rm.r2)

// For Random forest - crime type 9, 5 features - 0.6118806194993
val lrHeader = Seq("temp", "rain", "snow", "fog", "humidity", "freq")
val lrDF = Crime9rdd.map(line => (line(0),  line(1), line(2), line(3), line(4), line(5))).toDF(lrHeader: _*)
val castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("freq").cast(IntegerType).as("freq"))
val cols = Array("temp", "rain", "snow", "fog", "humidity")

val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featuresDF = assembler.transform(castLrDF)
val seed = 5043
val Array(trainData, testData) = featuresDF.randomSplit(Array(0.7, 0.3), seed)
val rf = new RandomForestRegressor().setSeed(seed).setLabelCol("freq").setFeaturesCol("features").setNumTrees(100).setMaxDepth(10)

val rfModel = rf.fit(featuresDF)
rfModel.write.overwrite().save("/user/vag273/project/RF_9MOD")

val predictDF = rfModel.transform(testData)
val TruePredict = predictDF.rdd.map(line => (line(5).toString.toDouble, line(7).toString.toDouble))
val rm = new RegressionMetrics(TruePredict)

println(rm.r2)

// For Random forest - crime type 45, 5 features - 0.6018696148545717
val lrHeader = Seq("temp", "rain", "snow", "fog", "humidity", "freq")
val lrDF = Crime45rdd.map(line => (line(0),  line(1), line(2), line(3), line(4), line(5))).toDF(lrHeader: _*)
val castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("freq").cast(IntegerType).as("freq"))
val cols = Array("temp", "rain", "snow", "fog", "humidity")

val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featuresDF = assembler.transform(castLrDF)
val seed = 5043
val Array(trainData, testData) = featuresDF.randomSplit(Array(0.7, 0.3), seed)
val rf = new RandomForestRegressor().setSeed(seed).setLabelCol("freq").setFeaturesCol("features").setNumTrees(100).setMaxDepth(10)

val rfModel = rf.fit(featuresDF)
rfModel.write.overwrite().save("/user/vag273/project/RF_45MOD")

val predictDF = rfModel.transform(testData)
val TruePredict = predictDF.rdd.map(line => (line(5).toString.toDouble, line(7).toString.toDouble))
val rm = new RegressionMetrics(TruePredict)

println(rm.r2)

















