package com.adu21.kafka.demo;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

/**
 * @author LukeDu
 * @date 2023/2/2
 */
public class StreamSample {

    private static final String INPUT_TOPIC = "yidong-stream-in";
    private static final String OUTPUT_TOPIC = "yidong-stream-out";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //如何构建流结构拓扑
        StreamsBuilder builder = new StreamsBuilder();
        //构建wordcount processor
        wordcountStream2(builder);
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
    }

    //如何定义流计算过程
    public static void wordcountStream(StreamsBuilder builder) {
        //KStream 不断从INPUT_TOPIC上获取新数据，并且追加到流上的一个抽象对象
        KStream<String, String> source = builder.stream(INPUT_TOPIC);
        //KTable 是数据集合的抽象对象，
        KTable<String, Long> count = source
            //flatMapValues 数据拆分，将一行数据拆分为多行数据
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
            //合并 按value值进行合并
            .groupBy((key, value) -> value)
            //统计出现的总数
            .count();

        //将结果输入到OUTPUT_TOPIC中
        count.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

    //如何定义流计算过程
    public static void wordcountStream2(StreamsBuilder builder) {
        //KStream 不断从INPUT_TOPIC上获取新数据，并且追加到流上的一个抽象对象
        KStream<String, String> source = builder.stream(INPUT_TOPIC);
        source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
            .foreach((key, value) -> System.out.println(key + " : " + value));

    }
}