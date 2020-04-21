import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
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
    val trimDF = sqlCtx.sql("""SELECT * FROM table WHERE type = 3 LIMIT 70000""").union(sqlCtx.sql("""SELECT * FROM table WHERE type = 9 LIMIT 70000""")).union(sqlCtx.sql("""SELECT * FROM table WHERE type = 42 LIMIT 70000""")).union(sqlCtx.sql("""SELECT * FROM table WHERE type = 45 LIMIT 70000"""))

    val cols = Array("temp", "rain", "snow", "fog", "humidity")
    val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
    val indexer = new StringIndexer().setInputCol("type").setOutputCol("label")

    val seed = 5043
    val Array(trainData, testData) = trimDF.randomSplit(Array(0.8, 0.2), seed)
    val randomForestClassifier = new RandomForestClassifier().setMaxDepth(2).setNumTrees(128).setFeatureSubsetStrategy("auto").setImpurity("gini").setSubsamplingRate(0.8).setSeed(seed)

    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("plabel").setLabels(indexer.labels)

    val stages = Array(assembler, indexer, randomForestClassifier)

    val pipeline = new Pipeline().setStages(stages)
    val pipelineModel = pipeline.fit(trainData)
    pipelineModel.write.overwrite().save("/user/vag273/project/model")

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")

    val predictDF = cvModel.transform(testData)

    val accuracy = evaluator.evaluate(predictDF)
    println("accuracy: " + accuracy)
  }
}
