package com.flight_analytics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.KeyValue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class StreamProcessor {

	private static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static final String FLIGHTS_DATA_TOPIC = "flights-data";
	public static final String FLIGHTS_LOOKUP_TOPIC = "flights-lookup";
	public static final String FLIGHTS_PROCESSED_TOPIC = "flights-processed";
	public static final String AIRLINE_COUNTS_TOPIC = "airline-aircraft-counts";

	/* Window size for "live" airline aircraft count (distinct icao24 per airline in this window).
	 *  Results are emitted only when the window closes (suppression); topic stays empty until first window completes.
	 */
	private static final Duration AIRLINE_COUNT_WINDOW_SIZE = Duration.ofSeconds(10);
	public static final String AIRPORTS_LOOKUP_TOPIC = "airports-lookup";


	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-processor");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> inputStream = builder.stream(FLIGHTS_DATA_TOPIC);
		GlobalKTable<String, String> flightsLookupTable = builder.globalTable(FLIGHTS_LOOKUP_TOPIC);

		KStream<String, String> transformedStream = inputStream
				.selectKey((key, v) -> getIcao24FromStream(v))
				.filter((key, value) -> key != null && !key.isBlank());

		transformedStream
				.leftJoin(
					flightsLookupTable,
					(streamKey, streamValue) -> streamKey,
					StreamProcessor::joinWithFlightsLookup
				)
				.filter((key, value) -> value != null)
				.to(FLIGHTS_PROCESSED_TOPIC);

		transformedStream
				.filter((key, value) -> extractAirlineCodeFromValue(value) != null)
				.groupBy((key, value) -> extractAirlineCodeFromValue(value))
				.windowedBy(TimeWindows.ofSizeWithNoGrace(AIRLINE_COUNT_WINDOW_SIZE))
				.aggregate(
						HashSet::new,
						(key, value, icao24Set) -> {
							String icao24 = getIcao24FromStream(value);
							if (icao24 != null) icao24Set.add(icao24);
							return icao24Set;
						},
						Materialized.with(Serdes.String(), setSerde())
				)
				.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
				.toStream()
				.mapValues(Set::size)
				.map(StreamProcessor::airlineCountToKeyValue)
				.to(AIRLINE_COUNTS_TOPIC);

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

		System.out.println("Starting Kafka Streams: (" + FLIGHTS_DATA_TOPIC + " + " + FLIGHTS_LOOKUP_TOPIC + ") -> processing -> " + FLIGHTS_PROCESSED_TOPIC);
		streams.start();
	}

	private static String getIcao24FromStream(String streamValue) {
		if (streamValue == null || streamValue.isBlank())
			return null;

		try {
			JsonNode root = MAPPER.readTree(streamValue);
			if (!root.isObject())
				return null;

			JsonNode icao = root.get("icao24");

			if (icao != null && !icao.isNull()) {
				String s = icao.asText(null);

				if (s != null && !s.isBlank())
					return s.trim().toLowerCase();
			}

			return null;
		} catch (Exception e) {

			return null;
		}
	}

	private static String joinWithFlightsLookup(String streamValue, String lookupValue) {
		if (streamValue == null || streamValue.isBlank())
			return null;

		try {
			JsonNode root = MAPPER.readTree(streamValue);
			if (!root.isObject())
				return streamValue;

			ObjectNode obj = (ObjectNode) root;

			if (lookupValue != null && !lookupValue.isBlank()) {
				JsonNode lookup = MAPPER.readTree(lookupValue);
				if (lookup.isObject()) {
					String icao24 = lookup.has("icao24") ? lookup.get("icao24").asText() : root.path("icao24").asText("?");

					System.out.println("Join by icao24: " + icao24 + " callsign=" + root.path("callsign").asText("?"));

					if (lookup.has("AvgCarrierDelayMinutes") && !lookup.get("AvgCarrierDelayMinutes").isNull())
						obj.put("avg_carrier_delay_minutes", lookup.get("AvgCarrierDelayMinutes").asDouble());

					if (lookup.has("FlightCount"))
						obj.put("lookup_flight_count", lookup.get("FlightCount").asInt());

					if (lookup.has("DelayScore01") && !lookup.get("DelayScore01").isNull())
						obj.put("delay_score_01", lookup.get("DelayScore01").asDouble());
				}
			}

			String icao24 = root.has("icao24") ? root.get("icao24").asText() : "?";
			obj.put("_id", icao24);
			return MAPPER.writeValueAsString(obj);

		} catch (Exception e) {
			System.err.println("Join FAILED: " + e.getMessage());
			return streamValue;
		}
	}

	public static String extractAirlineCode(String callsign) {
		if (callsign == null || callsign.isBlank())
			return "UNKNOWN";

		String cs = callsign.trim().toUpperCase();

		// First three letters determine airline code
		// callsign starting with N are treated as private flights
		if (cs.isEmpty())
			return "UNKNOWN";
		if (cs.charAt(0) == 'N')
			return "PRIVATE";
		if (cs.length() < 3)
			return cs;

		return cs.substring(0, 3);
	}

	public static String extractAirlineCodeFromValue(String jsonValue) {
		if (jsonValue == null || jsonValue.isBlank())
			return null;

		try {
			JsonNode root = MAPPER.readTree(jsonValue);
			JsonNode callsign = root.path("callsign");

			if (callsign.isMissingNode() || callsign.isNull())
				return null;

			String cs = callsign.asText(null);
			return cs != null ? extractAirlineCode(cs) : null;

		} catch (Exception e) {
			return null;
		}
	}

	private static KeyValue<String, String> airlineCountToKeyValue(Windowed<String> windowedKey, Integer count) {
		String airline = windowedKey.key();
		long windowEndMs = windowedKey.window().end();
		ObjectNode obj = MAPPER.createObjectNode();

		obj.put("_id", airline);
		obj.put("airline", airline);
		obj.put("aircraft_count", count);
		obj.put("window_end_ms", windowEndMs);

		try {
			String value = MAPPER.writeValueAsString(obj);
			return KeyValue.pair(airline, value);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to serialize airline count", e);
		}
	}

	private static Serde<Set<String>> setSerde() {
		Serializer<Set<String>> ser = (topic, set) ->
				(set == null ? "" : String.join(",", set)).getBytes(StandardCharsets.UTF_8);

		Deserializer<Set<String>> des = (topic, bytes) -> {
			if (bytes == null || bytes.length == 0)
				return new HashSet<>();

			String s = new String(bytes, StandardCharsets.UTF_8);
			if (s.isEmpty())
				return new HashSet<>();

			return new HashSet<>(Arrays.asList(s.split(",", -1)));
		};

		return Serdes.serdeFrom(ser, des);
	}
}
