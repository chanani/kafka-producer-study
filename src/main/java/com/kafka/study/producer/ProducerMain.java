package com.kafka.study.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerMain {

    private final static Logger logger = LoggerFactory.getLogger(ProducerMain.class);
    private final static String TOPIC_NAME = "test"; // 토픽명
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092"; // 클러스터명

    // 메인 실행했을 때 내부 send 로직 작동
    /* 확인 데이터 확인 방법
     bin/kafka-console-consumer.sh --bootstrap-server my-kafka:9092 --topic test --from-beginning
     */
    public static void main(String[] args) {

        Properties configs = new Properties(); // 설정 객체 생성
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); // 설정한 내용 config에 넣기
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // key 직렬화 방법
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // value 직렬화 방법

        // 레코드 객체 생생(직렬화에 StringSerializer를 사용했기 때문에 객체의 매개변수 타입은 String)
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String messageValue = "testMessage"; // 레코드 값
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue); // 레코드 객체 생성
        producer.send(record); // send
        logger.info("{}", record); // log
        producer.flush(); // 일정 시간이 되면 send가 발송 되지만 flush로 강제 전송
        producer.close(); // 종료
    }
}
