package com.kafka.study.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerKeyValueMain {

    private final static String TOPIC_NAME = "test"; // 토픽명
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092"; // 클러스터명

    // producer에서 send 시 key value 추가
    /* 확인 데이터 확인 방법
        bin/kafka-console-consumer.sh --bootstrap-server my-kafka:9092 --topic test
        --property print.key=true --property key.separator="-" --from-beginning
     */
    public static void main(String[] args) {

        Properties configs = new Properties(); // 설정 객체 생성
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); // 설정한 내용 config에 넣기
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // 직렬화 방법
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // 역직렬화 방법

        // 레코드 객체 생생(직렬화에 StringSerializer를 사용했기 때문에 객체의 매개변수 타입은 String)
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        // message key가 포함된 형태 (토픽명, key, value)
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Pangyo", "Pangyo");
        producer.send(record);
        ProducerRecord<String, String> record2 = new ProducerRecord<>(TOPIC_NAME, "Busan", "Busan");
        producer.send(record2);
        producer.flush();
        producer.close();
    }


}
