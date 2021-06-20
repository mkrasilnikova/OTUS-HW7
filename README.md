# OTUS-HW7


Построение модели

$SPARK_HOME/bin/spark-submit --class MLModel   homework7-assembly-0.1.jar

Запуск приложения

$SPARK_HOME/bin/spark-submit --class MLStructuredStreaming  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 homework7-assembly-0.1.jar

Запуск consumer'а

bin/kafka-console-consumer.sh --topic prediction --bootstrap-server localhost:9092

Запуск producer'а

awk -F ',' 'NR > 1 {print $1 "," $2 "," $3 "," $4}' < ~/otus/homework7/data/IRIS.csv | bin/kafka-console-producer.sh --topic input --bootstrap-server localhost:9092