import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;


public class A4Application {

    public static void main(String[] args) throws Exception {
	// do not modify the structure of the command line
	String bootstrapServers = args[0];
	String appName = args[1];
	String studentTopic = args[2];
	String classroomTopic = args[3];
	String outputTopic = args[4];
	String stateStoreDir = args[5];

	Properties props = new Properties();
	props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
	props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
	props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);

	// add code here if you need any additional configuration options

	StreamsBuilder builder = new StreamsBuilder();

	KStream<String, String> studentLocationStreams = builder.stream(studentTopic);
	KStream<String, String> classroomCapacities = builder.stream(classroomTopic);

	KTable<String, Integer> classCapacities = classroomCapacities
		.map((roomNumber, capacity) -> KeyValue.pair(roomNumber, Integer.parseInt(capacity)))
		.groupByKey(
			Serialized.with(
				Serdes.String(),
				Serdes.Integer())
		)
		.reduce(
			(aggValue, newValue) -> newValue,
			Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("class-capacity-store")
		);

	KTable<String, Integer> studentLocations = studentLocationStreams
		.groupByKey(
			Serialized.with(
				Serdes.String(),
				Serdes.String())
		)
		.reduce(
			(aggValue, newValue) -> {
				return newValue;
			}, /* adder */
			Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("student-location-store")
		)
		.groupBy(
			(studentName, roomNumber) ->
			{
				System.out.println(studentName + roomNumber);
				return KeyValue.pair(roomNumber, 1);
			},
			Grouped.with(
				Serdes.String(),
				Serdes.Integer()
			)
		)
		.aggregate(
			() -> 0, /* initializer */
			(aggKey, newValue, aggValue) -> {
				System.out.println("Add to " + aggKey);
				return aggValue + newValue;
			},
			(aggKey, oldValue, aggValue) -> {
				System.out.println("Subtract From " + aggKey);
				return aggValue - oldValue;
			},
			Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("current-class-capacity" /* state store name */)
					.withKeySerde(Serdes.String())
					.withValueSerde(Serdes.Integer())
		);


	KTable<String, String> joinedTable = classCapacities
		.join(studentLocations,
			(maxCapacity, currentSize) -> maxCapacity + "-" + currentSize
		)
		.groupBy(
			(classroom, capacities) -> KeyValue.pair(classroom, capacities),
			Grouped.with(
					Serdes.String(),
					Serdes.String()
			)
		)
		.aggregate(
			() -> {
				String[] s = newValue.split("-");
				int maxCapacity = Integer.parseInt(s[0]);
				int currentSize = Integer.parseInt(s[1]);

				if(currentSize > maxCapacity) {
					return currentSize.toString();
				} else if (currentSize == maxCapacity && !aggValue.equals("") && !aggValue.equals("OK")){
					return "OK";
				} else {
					return "";
				}
			},
			(aggKey, newValue, aggValue) -> {
				String[] s = newValue.split("-");
				int maxCapacity = Integer.parseInt(s[0]);
				int currentSize = Integer.parseInt(s[1]);

				if(currentSize > maxCapacity) {
					return currentSize.toString();
				} else if (currentSize == maxCapacity && !aggValue.equals("") && !aggValue.equals("OK")){
					return "OK";
				} else {
					return "";
				}
			},
			(aggKey, oldValue, aggValue) -> aggValue,
			Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("output-store" /* state store name */)
					.withKeySerde(Serdes.String())
					.withValueSerde(Serdes.String())
		);


	joinedTable.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

// ...
// ...to(outputTopic);

	KafkaStreams streams = new KafkaStreams(builder.build(), props);

	// this line initiates processing
	streams.start();

	// shutdown hook for Ctrl+C
	Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
