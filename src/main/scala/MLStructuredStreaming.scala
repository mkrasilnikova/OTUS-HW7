import com.typesafe.config._
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.concat_ws


object MLStructuredStreaming {
  def main (args:Array[String]):Unit = {
  val config = ConfigFactory.load
    val inputBootstrapServers = config.getString("input.bootstrap.servers")
    val inputTopic = config.getString("input.topic")
    val outputBootstrapServers = config.getString("output.bootstrap.servers")
    val outputTopic = config.getString("output.topic")
    val pathToModel = config.getString("pathToModel")
    val checkpointLocation = config.getString("checkpointLocation")

    val spark = SparkSession.builder
      .appName("MLStructuredStreaming")
      .getOrCreate()

    import spark.implicits._

    val model = CrossValidatorModel.load(pathToModel)

    val input = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", inputBootstrapServers)
      .option("subscribe", inputTopic)
      .load()
      .selectExpr("CAST (value AS STRING)")
      .as[String]
      .map(_.split(","))
      .map(Data(_))



    val prediction = model.transform(input)

    val query = prediction
      .select(concat_ws(",", $"sepal_length", $"sepal_width", $"petal_length", $"petal_width", $"predictedSpecies").as("value"))
      .writeStream
      .option("checkpointLocation", checkpointLocation)
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", outputBootstrapServers)
      .option("topic", outputTopic)
      .start()

    query.awaitTermination()



  }



}
