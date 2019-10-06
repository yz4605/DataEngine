package stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Properties;

public class DataStream {

    static String APPLICATION_ID = "application";
    static String INPUT_TOPIC="order";
    static String OUTPUT_TOPIC="price";

    public static void runStream(String broker) {

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> record = builder.stream(INPUT_TOPIC);

        KStream afterProcess = record
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(1)))
                .reduce((val1,val2)->val1+val2)
                .toStream();

        afterProcess.to(OUTPUT_TOPIC, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
    }

}
