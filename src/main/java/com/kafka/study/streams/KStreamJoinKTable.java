package com.kafka.study.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

// KTable과 KStream 조인

/**
 * 시작 전 준비(파티션은 꼭 동일하게 생성해야된다.)
 * 1. address topic 생성 : bin/kafka-topics.sh --bootstrap-server my-kafka:9092 --partitions 3 --topic address --create
 * 2. order topic 생성 : bin/kafka-topics.sh --bootstrap-server my-kafka:9092 --partitions 3 --topic order --create
 * 3. order-join topic 생성 : bin/kafka-topics.sh --bootstrap-server my-kafka:9092 --partitions 3 --topic order-join --create
 *
 * address topic에 레코드 추가 : bin/kafka-console-producer.sh --bootstrap-server my-kafka:9092 --topic address --property "parse.key=true" --property "key.separator=:"
 * order topic에 레코드 추가 : bin/kafka-console-producer.sh --bootstrap-server my-kafka:9092 --topic order --property "parse.key=true" --property "key.separator=:"
 * 데이터 Key:value로 추가 후 결과 확인
 * bin/kafka-console-consumer.sh --bootstrap-server my-kafka:9092 --topic order-join --property print.key=true --property key.separator=":" --from-beginning
 */
public class KStreamJoinKTable {

    private static String APPLICATION_NAME = "order-join-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String ADDRESS_TABLE = "address";
    private static String ORDER_STREAM = "order";
    private static String ORDER_JOIN_STREAM = "order-join";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> addressTable = builder.table(ADDRESS_TABLE); // 메시지 키를 기준
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM); // order가 들어오면 join 시켜서 ORDER JOIN에 들어간다.

        orderStream.join(addressTable, (order, address) -> order + " send to " + address).to(ORDER_JOIN_STREAM);

        KafkaStreams streams;
        streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}


