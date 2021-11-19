import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr, from_json, udf}
import org.apache.spark.sql.streaming.Trigger
import sample.utils.UdpStream
import configs.{DEV, PROD}

object Apps {
  var udp_socket: UdpStream = null;
  var udp_socket_switch: Boolean = true;

  def main(args: Array[String]): Unit = {
    println(s"하둡경로지정 ${DEV.HADOOP_HOME_DIR}");
    System.setProperty("hadoop.home.dir", DEV.HADOOP_HOME_DIR);
    udp_socket = new UdpStream(DEV.UDP_HOST_PORT);

    //세션 생성
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName(DEV.KAFKA_SUBSCRIBE_TOPICS)
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN");
    val sc: SparkContext = spark.sparkContext;
    import spark.implicits._
    println(s"SPARK SESSION 생성됨 SPARK 모니터링 서비스 PORT=4040");

    //카프카 연결
    val linesDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"${DEV.KAFKA_HOST_IP}:${DEV.KAFKA_HOST_PORT}")
      .option("subscribe", s"${DEV.KAFKA_SUBSCRIBE_TOPICS}")
      .option("stopGracefullyOnShutdown", "true")
      .load()
    println(s"KAFKA 서버 호스트 연결 및 구독 성공 ${DEV.KAFKA_HOST_IP}:${DEV.KAFKA_HOST_PORT},${DEV.KAFKA_SUBSCRIBE_TOPICS}");

    val toStr = udf((payload: Array[Byte]) => new String(payload))
    val parsing = linesDF.withColumn("value", toStr(linesDF("value")))

    //TODO 카프카 구독정보를 실시간으로 받아옴, Trigger.ProcessingTime로 interval 주기 설정
    println(s"KAFKA 리시버 동작중 ..");
    val kafka_receiver = parsing.writeStream
      .format("console")
      .option("checkpointLocation", "chk-point-dir");

    kafka_receiver
      .trigger(Trigger.ProcessingTime(DEV.KAFKA_SUBSCRIBE_INTERVAL_MILLISECOND))
      .foreachBatch(myFunc _)
      .start()
      .awaitTermination();
  }

  //TODO scala 12 부터 foreachBatch 이슈에 대한 대응
  //http://jason-heo.github.io/bigdata/2021/01/23/spark30-foreachbatch.html
  def myFunc(batch: DataFrame, batchId: Long): Unit = {
    try {
      udp_socket.kafka_data = batch.first().json;
      println(udp_socket.kafka_data);
      if (udp_socket_switch) {
        udp_socket_switch = false;
        udp_socket.startServer();
      }
    } catch {
      case e1: NoSuchElementException => {
        println(s"KAFKA 수신된 패킷이 존재하지 않아 UDP HOST 생성을 중지함");
      }
    }
  }
}