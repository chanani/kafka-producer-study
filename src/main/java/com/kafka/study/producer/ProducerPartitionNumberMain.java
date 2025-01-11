package com.kafka.study.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerPartitionNumberMain {

    private final static String TOPIC_NAME = "test"; // 토픽명
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092"; // 클러스터명

    // producer에서 send 시 파티션 번호 지정해서 send
    /* 확인 데이터 확인 방법
        bin/kafka-console-consumer.sh --bootstrap-server my-kafka:9092 --topic test
        --property print.key=true --property key.separator="-" --from-beginning
     */
    public static void main(String[] args) {

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        // message 파티션 번호를 포함한 형태 (토픽명, 파티션 번호, key, value)
        int partitionNo = 0;
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNo, "Pangyo", "Pangyo");
        producer.send(record);

        producer.flush();
        producer.close();
    }


}
