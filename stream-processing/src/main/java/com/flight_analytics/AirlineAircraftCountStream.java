package com.flight_analytics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class AirlineAircraftCountStream {

	public static final String OUTPUT_TOPIC = "airline-aircraft-counts";

	private static final Duration WINDOW_SIZE = Duration.ofSeconds(10);
	private static final ObjectMapper MAPPER = new ObjectMapper();

	private AirlineAircraftCountStream() {}

	public static void addTo(StreamsBuilder builder, KStream<String, String> transformedStream) {
		transformedStream
				.filter((key, value) -> extractAirlineCodeFromValue(value) != null)
				.groupBy((key, value) -> extractAirlineCodeFromValue(value))
				.windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE))
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
				.map(AirlineAircraftCountStream::airlineCountToKeyValue)
				.to(OUTPUT_TOPIC);
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

	public static String extractAirlineCode(String callsign) {
		if (callsign == null || callsign.isBlank())
			return "UNKNOWN";
		String cs = callsign.trim().toUpperCase();
		if (cs.isEmpty())
			return "UNKNOWN";
		if (cs.charAt(0) == 'N')
			return "PRIVATE";
		if (cs.length() < 3)
			return cs;
		return cs.substring(0, 3);
	}

	private static String extractAirlineCodeFromValue(String jsonValue) {
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
