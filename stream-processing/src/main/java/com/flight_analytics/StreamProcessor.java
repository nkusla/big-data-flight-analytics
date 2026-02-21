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

	public static final String FLIGHTS_DATA_TOPIC = "flights-data";
	public static final String FLIGHTS_LOOKUP_TOPIC = "flights-lookup";
	public static final String AIRCRAFTS_LOOKUP_TOPIC = "aircrafts-lookup";
	public static final String FLIGHTS_PROCESSED_TOPIC = "flights-processed";

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-processor");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();

		GlobalKTable<String, String> aircraftsLookupTable = builder.globalTable(AIRCRAFTS_LOOKUP_TOPIC);
		GlobalKTable<String, String> flightsLookupTable = builder.globalTable(FLIGHTS_LOOKUP_TOPIC);
		KStream<String, String> inputStream = builder.stream(FLIGHTS_DATA_TOPIC);

		KStream<String, String> transformedStream = inputStream
				.selectKey((key, v) -> getIcao24FromStream(v))
				.filter((key, value) -> key != null && !key.isBlank());

		transformedStream
				.leftJoin(
					aircraftsLookupTable,
					(streamKey, streamValue) -> streamKey,
					StreamProcessor::joinWithAircraftsLookup
				)
				.leftJoin(
					flightsLookupTable,
					(streamKey, streamValue) -> getCallsignFromStream(streamValue),
					StreamProcessor::joinWithFlightsLookup
				)
				.filter((key, value) -> value != null)
				.to(FLIGHTS_PROCESSED_TOPIC);

		AirlineAircraftCountStream.addTo(builder, transformedStream);
		AirportAircraftCountStream.addTo(builder, transformedStream);

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

		System.out.println("\nStarting Kafka Streams: ");
		System.out.println("\tUsing lookup topics: " + AIRCRAFTS_LOOKUP_TOPIC + ", " + FLIGHTS_LOOKUP_TOPIC + ", " + AirportAircraftCountStream.AIRPORTS_LOOKUP_TOPIC);
		System.out.println("\t" + FLIGHTS_DATA_TOPIC + " -> " + FLIGHTS_PROCESSED_TOPIC + ", " + AirlineAircraftCountStream.OUTPUT_TOPIC + ", " + AirportAircraftCountStream.OUTPUT_TOPIC);
		System.out.println();

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

	private static String getCallsignFromStream(String streamValue) {
		if (streamValue == null || streamValue.isBlank())
			return null;
		try {
			JsonNode root = MAPPER.readTree(streamValue);
			if (!root.isObject())
				return null;
			JsonNode callsign = root.get("callsign");
			if (callsign == null || callsign.isNull())
				return null;
			String s = callsign.asText(null);
			return (s != null && !s.isBlank()) ? s.trim() : null;
		} catch (Exception e) {
			return null;
		}
	}

	private static String joinWithAircraftsLookup(String streamValue, String lookupValue) {
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

					System.out.println("Join by callsign: " + lookup.get("callsign").asText());

					if (lookup.has("callsign") && !lookup.get("callsign").isNull())
						obj.put("callsign", lookup.get("callsign").asText());

					if (lookup.has("AirlineName") && !lookup.get("AirlineName").isNull())
						obj.put("AirlineName", lookup.get("AirlineName").asText());

					if (lookup.has("CRSArrTime") && !lookup.get("CRSArrTime").isNull())
						obj.put("CRSArrTime", lookup.get("CRSArrTime").asText());

					if (lookup.has("CRSDepTime") && !lookup.get("CRSDepTime").isNull())
						obj.put("CRSDepTime", lookup.get("CRSDepTime").asText());
				}
			}
			return MAPPER.writeValueAsString(obj);
		} catch (Exception e) {
			System.err.println("Flights lookup join FAILED: " + e.getMessage());
			return streamValue;
		}
	}

}
