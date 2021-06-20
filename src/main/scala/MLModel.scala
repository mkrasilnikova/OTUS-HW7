import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, MinMaxScaler, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}

object MLModel {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load
    val dataPath = config.getString("dataPath")
    val spark = SparkSession.builder
      .appName("MLModel")
      .getOrCreate()

    val df = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataPath + "IRIS.csv")

    df.printSchema()
    val numericColumns = df.dtypes.filter(!_._2.equals("StringType")).map(d => d._1)
    df.select(numericColumns.map(col): _*).summary().show()

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("species")
      .setOutputCol("indexedSpecies")
      .fit(df)

    val pairs = numericColumns
      .flatMap(f1 => numericColumns.map(f2 => (f1, f2)))
      .filter(p => !p._1.equals(p._2))
      .map(p => if (p._1 < p._2) (p._1, p._2) else (p._2, p._1))
      .distinct

    val corr = pairs
      .map(p => (p._1, p._2, df.stat.corr(p._1, p._2)))
      .filter(_._3 > 0.6)

    corr.sortBy(_._3).reverse.foreach(c => println(f"${c._1}%25s${c._2}%25s\t${c._3}"))

    val featureColumns = numericColumns.diff(corr.map(_._2))

    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("features")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("scaledFeatures")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedSpecies")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(1000)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedSpecies")
      .setLabels(labelIndexer.labelsArray(0))

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, assembler, scaler, featureIndexer, rf, labelConverter))

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(1, 10, 100, 1000))
      .build()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedSpecies")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)
      .setParallelism(2)

    val cvModel = cv.fit(trainingData)

    val predictions = cvModel.transform(testData)

//  Select example rows to display.
    predictions.select("predictedSpecies", "species", "features").show(150)

    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${(1.0 - accuracy)}")

    cvModel.write.overwrite().save(dataPath + "/pipelineModel")

  }

}
