var featStr = ""
for (temp <- 0 to 100 by 10) {
  for (rain <- 0 to 1) {
    for (snow <- 0 to 1) {
      for (fog <- 0 to 1) {
        for (humid <- 10 to 100 by 10) {
          featStr += temp + "," + temp * temp + "," + rain + "," + snow + "," + fog + "," + humid + "," + humid * humid + "\n"
        }
      }
    }
  }
}

val featRDD = sc.parallelize(featStr.split("\n")).map(line => line.split(','))
val header = Seq("temp", "temp^2", "rain", "snow", "fog", "humidity", "humidity^2")
val featDF = featRDD.map(line => (line(0), line(1), line(2), line(3), line(4), line(5), line(6))).toDF(header: _*)
val castFDF = featDF.select(featDF("temp").cast(IntegerType).as("temp"), featDF("temp^2").cast(IntegerType).as("temp^2"), featDF("rain").cast(IntegerType).as("rain"), featDF("snow").cast(IntegerType).as("snow"), featDF("fog").cast(IntegerType).as("fog"), featDF("humidity").cast(IntegerType).as("humidity"), featDF("humidity^2").cast(IntegerType).as("humidity^2")) 

val cols = Array("temp", "temp^2", "rain", "snow", "fog", "humidity", "humidity^2")
val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val features = assembler.transform(castFDF)

val rf = RandomForestRegressor.load("/user/vag273/project/models/
