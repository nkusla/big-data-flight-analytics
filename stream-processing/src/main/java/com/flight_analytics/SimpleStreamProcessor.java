package com.flight_analytics;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * Simple Kafka Streams application that reads from 'input-topic',
 * transforms messages to uppercase, and writes to 'output-topic'.
 */
public class SimpleStreamProcessor {

    public static void main(String[] args) {
        // Configure the Streams application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-stream-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker-1:9092,kafka-broker-2:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Build the processing topology
        StreamsBuilder builder = new StreamsBuilder();

        // Read from input topic
        KStream<String, String> inputStream = builder.stream("input-topic");

        // Transform: convert message values to uppercase
        KStream<String, String> transformedStream = inputStream.mapValues(value -> {
            if (value != null) {
                System.out.println("Processing: " + value);
                return value.toUpperCase();
            }
            return null;
        });

        // Write to output topic
        transformedStream.to("output-topic");

        // Start the Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("Starting Kafka Streams application...");
        streams.start();
    }
}
