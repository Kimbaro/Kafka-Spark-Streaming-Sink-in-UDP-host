package configs


/*
* @var : KAFKA_HOST_IP                          카프카서버와 연결할 IP주소
  @var : KAFKA_HOST_PORT                        카프카서버와 연결할 포트
  @var : KAFKA_SUBSCRIBE_TOPICS                 카프카브로커에 구독할 정보
  @var : KAFKA_SUBSCRIBE_INTERVAL_MILLISECOND   카프카세션에 해당 쿼리 동작을 기록하기 위한 주기
  @var : UDP_HOST_PORT                          UDP 호스트 포트를 구성
  @var : HADOOP_HOME_DIR                        하둡 라이브러리 디렉토리 경로
*
* */
object DEV {
  var KAFKA_HOST_IP: String = "192.168.0.210"
  var KAFKA_HOST_PORT: String = "9092"
  var KAFKA_SUBSCRIBE_TOPICS: String = "test,test2,test3"
  var KAFKA_SUBSCRIBE_INTERVAL_MILLISECOND: Int = 0;
  var UDP_HOST_PORT: Int = 9998
  var HADOOP_HOME_DIR: String = "C:\\winutils\\hadoop-3.2.2"
}
