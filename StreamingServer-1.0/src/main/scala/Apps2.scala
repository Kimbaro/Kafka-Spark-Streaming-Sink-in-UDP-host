//import org.apache.spark.SparkContext
//import org.apache.spark.sql.functions.expr
//import org.apache.spark.sql.streaming.Trigger
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import sample.utils.HttpStream
//
//object Apps2 {
//  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "C:\\winutils\\hadoop-3.2.2");
//
//    val spark = SparkSession.builder()
//      .master("local[3]")
//      .appName("Streaming Word Count")
//      .config("spark.streaming.stopGracefullyOnShutdown", "true")
//      .config("spark.sql.shuffle.partitions", 3)
//      .getOrCreate()
//    spark.sparkContext.setLogLevel("WARN");
//    val sc: SparkContext = spark.sparkContext;
//
//    // 소켓
//    //    val linesDF = spark.readStream
//    //      .format("socket")
//    //      .option("host", "192.168.0.210")
//    //      .option("port", "9091")
//    //      .load();
//
//    val linesDF = spark.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "192.168.0.210:9091")
//      .option("subscribe", "test,test2,test3")
//      .option("stopGracefullyOnShutdown", "true")
//      .load()
//
//    //DataFrame 생성
//    val wordsDF = linesDF.select(expr("explode(split(value,' ')) as word"));
//    wordsDF.createOrReplaceTempView("dykim");
//
//    //test-start
//    val wordsDF2 = linesDF.select(expr("explode(split(value,' ')) as word"));
//    new HttpStream().toDF(new org.apache.spark.sql.SQLContext(sc)).writeStream
//      .trigger(Trigger.ProcessingTime("5 seconds"))
//      .foreachBatch(myFunc _).start()
//    //test-end
//
//    val countsDF = wordsDF.groupBy("word").count();
//    val wordCountQuery = countsDF.writeStream
//      .format("console")
//      .queryName("dykim")
//      .option("checkpointLocation", "chk-point-dir")
//      .outputMode("complete")
//      .start()
//
//    printf("Listening to localhost:9999");
//    wordCountQuery.awaitTermination()
//  }
////http://jason-heo.github.io/bigdata/2021/01/23/spark30-foreachbatch.html
//  def myFunc(batch: DataFrame, batchId: Long): Unit = {
//    println(s"Processing batch: $batchId")
//    batch.show(true)
////    askDF.persist()
////    askDF.write.parquet("/src/main/scala/file.json")
////    askDF.unpersist()
//  }
//}
