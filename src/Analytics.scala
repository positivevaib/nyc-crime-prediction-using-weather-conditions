import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
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
    joinDF.registerTempTable("table")
    val trimDF = spark.sql("""SELECT * FROM table WHERE type = 3 OR type = 4 OR type = 8 OR type = 9 OR type = 11 OR type = 13 OR type = 30 OR type = 31 OR type = 34 OR type = 42 OR type = 45 OR type = 50 OR type = 54 OR type = 55 OR type = 63""")

    val cols = Array("temp", "rain", "snow", "fog", "humidity")
    val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
    val indexer = new StringIndexer().setInputCol("type").setOutputCol("label")

    val seed = 5043
    val Array(trainData, testData) = trimDF.randomSplit(Array(0.8, 0.2), seed)
    val randomForestClassifier = new RandomForestClassifier().setFeatureSubsetStrategy("auto").setSeed(seed)
    val stages = Array(assembler, indexer, randomForestClassifier)

    val pipeline = new Pipeline().setStages(stages)

    val paramGrid = new ParamGridBuilder().addGrid(randomForestClassifier.maxDepth, Array(4, 6, 8)).addGrid(randomForestClassifier.impurity, Array("entropy", "gini")).addGrid(randomForestClassifier.numTrees, Array(100, 150, 200)).build()

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")

    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(5)
    val cvModel = cv.fit(trainData)

    val predictDF = cvModel.transform(testData)

    val accuracy = evaluator.evaluate(predictDF) * 100
    println("accuracy: " + accuracy + "%") 

    cvModel.write.overwrite().save("/user/vag273/project/model")
  }
}
