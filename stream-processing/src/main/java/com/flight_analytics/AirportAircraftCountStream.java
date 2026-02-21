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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class AirportAircraftCountStream {

	public static final String AIRPORTS_LOOKUP_TOPIC = "airports-lookup";
	public static final String OUTPUT_TOPIC = "airport-aircraft-counts";

	public static final double RADIUS_KM = 50.0;

	private static final Duration WINDOW_SIZE = Duration.ofMinutes(2);
	private static final String AIRPORTS_STORE_NAME = "airports-store";
	private static final ObjectMapper MAPPER = new ObjectMapper();

	private AirportAircraftCountStream() {}


	public static void addTo(StreamsBuilder builder, KStream<String, String> transformedStream) {
		StoreBuilder<KeyValueStore<String, String>> airportsStoreBuilder = Stores.keyValueStoreBuilder(
				Stores.persistentKeyValueStore(AIRPORTS_STORE_NAME),
				Serdes.String(),
				Serdes.String());
		builder.addGlobalStore(
				airportsStoreBuilder,
				AIRPORTS_LOOKUP_TOPIC,
				Consumed.with(Serdes.String(), Serdes.String()),
				(ProcessorSupplier<String, String, Void, Void>) () -> new Processor<String, String, Void, Void>() {
					KeyValueStore<String, String> store;

					@Override
					public void init(ProcessorContext<Void, Void> context) {
						store = (KeyValueStore<String, String>) context.getStateStore(AIRPORTS_STORE_NAME);
					}

					@Override
					public void process(Record<String, String> record) {
						if (record.key() != null && record.value() != null)
							store.put(record.key(), record.value());
					}
				});

		transformedStream
				.filter((key, value) -> extractFlightPosition(value) != null)
				.process(
						(ProcessorSupplier<String, String, String, String>) () -> new Processor<String, String, String, String>() {
							ProcessorContext<String, String> context;
							KeyValueStore<String, String> store;

							@Override
							public void init(ProcessorContext<String, String> context) {
								this.context = context;
								this.store = (KeyValueStore<String, String>) context.getStateStore(AIRPORTS_STORE_NAME);
							}

							@Override
							public void process(Record<String, String> record) {
								FlightPosition pos = extractFlightPosition(record.value());
								if (pos == null || store == null) return;
								String icao24 = pos.icao24;
								long ts = record.timestamp();
								try (KeyValueIterator<String, String> it = store.all()) {
									while (it.hasNext()) {
										KeyValue<String, String> e = it.next();
										String airportCode = e.key;
										String airportJson = e.value;
										if (airportJson == null) continue;
										Double lat = getDoubleFromJson(airportJson, "latitude");
										Double lon = getDoubleFromJson(airportJson, "longitude");
										if (lat != null && lon != null && haversineKm(pos.lat, pos.lon, lat, lon) <= RADIUS_KM)
											context.forward(new Record<>(airportCode, icao24, ts));
									}
								}
							}
						})
				.groupByKey()
				.windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE))
				.aggregate(
						HashSet::new,
						(airportCode, icao24, set) -> {
							if (icao24 != null) set.add(icao24);
							return set;
						},
						Materialized.with(Serdes.String(), setSerde())
				)
				.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
				.toStream()
				.map(AirportAircraftCountStream::airportCountToKeyValue)
				.to(OUTPUT_TOPIC);
	}

	private static double haversineKm(double lat1, double lon1, double lat2, double lon2) {
		double R = 6371;
		double dLat = Math.toRadians(lat2 - lat1);
		double dLon = Math.toRadians(lon2 - lon1);
		double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
				+ Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
				* Math.sin(dLon / 2) * Math.sin(dLon / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		return R * c;
	}

	private static Double getDoubleFromJson(String json, String field) {
		if (json == null || field == null)
			return null;

		try {
			JsonNode node = MAPPER.readTree(json).get(field);
			return (node != null && !node.isNull() && node.isNumber()) ? node.asDouble() : null;
		} catch (Exception e) {
			return null;
		}
	}

	private static final class FlightPosition {
		final String icao24;
		final double lat;
		final double lon;

		FlightPosition(String icao24, double lat, double lon) {
			this.icao24 = icao24;
			this.lat = lat;
			this.lon = lon;
		}
	}

	private static FlightPosition extractFlightPosition(String streamValue) {
		if (streamValue == null || streamValue.isBlank()) return null;
		try {
			JsonNode root = MAPPER.readTree(streamValue);
			if (!root.isObject()) return null;
			String icao24 = extractIcao24(streamValue);
			Double lat = getDoubleFromJson(streamValue, "latitude");
			Double lon = getDoubleFromJson(streamValue, "longitude");
			if (icao24 == null || lat == null || lon == null) return null;
			return new FlightPosition(icao24, lat.doubleValue(), lon.doubleValue());
		} catch (Exception e) {
			return null;
		}
	}

	private static String extractIcao24(String streamValue) {
		if (streamValue == null || streamValue.isBlank()) {
			return null;
		}

		try {
			JsonNode root = MAPPER.readTree(streamValue);

			if (!root.isObject())
				return null;

			JsonNode icao = root.get("icao24");
			if (icao == null || icao.isNull())
				return null;
			String s = icao.asText(null);
			return (s != null && !s.isBlank()) ? s.trim().toLowerCase() : null;
		} catch (Exception e) {
			return null;
		}
	}

	private static KeyValue<String, String> airportCountToKeyValue(Windowed<String> windowedKey, Set<String> icao24Set) {
		String airportCode = windowedKey.key();
		int count = icao24Set != null ? icao24Set.size() : 0;
		long windowEndMs = windowedKey.window().end();
		ObjectNode obj = MAPPER.createObjectNode();
		obj.put("_id", airportCode);
		obj.put("AirportCode", airportCode);
		obj.put("aircraft_count", count);
		obj.put("window_end_ms", windowEndMs);
		try {
			return KeyValue.pair(airportCode, MAPPER.writeValueAsString(obj));
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to serialize airport count", e);
		}
	}

	private static Serde<Set<String>> setSerde() {
		Serializer<Set<String>> ser = (topic, set) ->
				(set == null ? "" : String.join(",", set)).getBytes(StandardCharsets.UTF_8);
		Deserializer<Set<String>> des = (topic, bytes) -> {
			if (bytes == null || bytes.length == 0) return new HashSet<>();
			String s = new String(bytes, StandardCharsets.UTF_8);
			if (s.isEmpty()) return new HashSet<>();
			return new HashSet<>(Arrays.asList(s.split(",", -1)));
		};
		return Serdes.serdeFrom(ser, des);
	}
}
