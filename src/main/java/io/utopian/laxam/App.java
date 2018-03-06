package io.utopian.laxam;

import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.KafkaStreams;

public class App
{
    public static void main( String[] args ) {
		Properties props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "WordCount");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,
				  Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,
				  Serdes.String().getClass().getName());

		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> input = builder.stream("text");

		KTable<String, Long> wordCounts = input
			.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
			.groupBy((key, value) -> value)
			.count();

		wordCounts.toStream().to("wordcount", Produced.with(Serdes.String(), Serdes.Long()));

		KafkaStreams streams = new KafkaStreams(builder, props);
		streams.start();
    }
}
