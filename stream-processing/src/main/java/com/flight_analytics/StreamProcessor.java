package com.flight_analytics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;

public class StreamProcessor {

	private static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static final String FLIGHTS_DATA_TOPIC = "flight-data";
	public static final String FLIGHTS_LOOKUP_TOPIC = "flights-lookup";

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-processor");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> inputStream = builder.stream(FLIGHTS_DATA_TOPIC);
		GlobalKTable<String, String> flightsLookupTable = builder.globalTable(FLIGHTS_LOOKUP_TOPIC);

		KStream<String, String> processedStream = inputStream
				.leftJoin(
						flightsLookupTable,
						(streamKey, streamValue) -> joinKeyFromStream(streamValue),
						StreamProcessor::mergeLookupAndProcess
				)
				.filter((key, value) -> value != null);

		processedStream.to("output-topic");

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

		System.out.println("Starting Kafka Streams: flight-data -> processing -> output-topic");
		streams.start();
	}

	private static String joinKeyFromStream(String streamValue) {
		if (streamValue == null || streamValue.isBlank()) return null;
		try {
			JsonNode root = MAPPER.readTree(streamValue);
			if (!root.isObject()) return null;
			JsonNode icao = root.get("icao24");
			if (icao != null && !icao.isNull()) {
				String s = icao.asText(null);
				if (s != null && !s.isBlank()) return s.trim().toLowerCase();
			}
			return null;
		} catch (Exception e) {
			return null;
		}
	}

	private static String mergeLookupAndProcess(String streamValue, String lookupValue) {
		if (streamValue == null || streamValue.isBlank()) return null;
		try {
			JsonNode root = MAPPER.readTree(streamValue);
			if (!root.isObject()) return streamValue;
			ObjectNode obj = (ObjectNode) root;

			if (lookupValue != null && !lookupValue.isBlank()) {
				JsonNode lookup = MAPPER.readTree(lookupValue);
				if (lookup.isObject()) {
					String icao24 = lookup.has("icao24") ? lookup.get("icao24").asText() : root.path("icao24").asText("?");
					System.out.println("Join by icao24: " + icao24 + " callsign=" + root.path("callsign").asText("?"));
					if (lookup.has("AvgCarrierDelayMinutes") && !lookup.get("AvgCarrierDelayMinutes").isNull())
						obj.put("avg_carrier_delay_minutes", lookup.get("AvgCarrierDelayMinutes").asDouble());
					if (lookup.has("FlightCount")) obj.put("lookup_flight_count", lookup.get("FlightCount").asInt());
					if (lookup.has("DelayScore01") && !lookup.get("DelayScore01").isNull())
						obj.put("delay_score_01", lookup.get("DelayScore01").asDouble());
				}
			}

			obj.put("processed_at", System.currentTimeMillis());
			String out = MAPPER.writeValueAsString(obj);
			return out;
		} catch (Exception e) {
			System.err.println("Processing failed for value: " + e.getMessage());
			return streamValue;
		}
	}
}
