package com.kafka.study.consumer;

import com.kafka.study.producer.ProducerWithSyncCallbackMain;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class ConsumerCommitAsyncMain {

    private final static Logger logger = LoggerFactory.getLogger(ProducerWithSyncCallbackMain.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test_group";

    // consumer 기본 로직(수동 커밋)
    // commitSnc()는 poll() 메서드로 받은 가장 마지막 레코드의 오프셋을 기준으로 커밋합니다.
    public static void main(String[] args) {

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 오토커밋 true(기본 값)

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record : {}", record);
            }

            // 비동기 오프셋 커밋
            consumer.commitAsync(new OffsetCommitCallback() {
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                    if (e != null)
                        System.out.println("Commit failed");
                    else
                        System.out.println("Commit succeeded");
                    if (e != null)
                        logger.error("Commit failed for offsets : {]", offsets, e);
                }
            });
        }


    }
}
