package kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class DataProducer {

    static String KAFKA_BROKERS = "localhost:9092";
    static String TOPIC_NAME="test";
    static String CLIENT_ID="client";

    public static Producer createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void runProducer(Producer producer,String ticker, int low, int high, long order) {
        IntStream percent = ThreadLocalRandom.current().ints(low, high).limit(order);
        percent.forEach(x -> {
            float num = 1 + (float) x / 10000;
            num = 100 * num;
            String price = String.format("%.4f%n", num);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, '['+ticker+']', price);
            System.out.println('['+ticker+']' + price);
            producer.send(record);
            //RecordMetadata metadata = producer.send(record).get();
            //System.out.println("Record sent with key " + index + " to partition " + metadata.partition() + " with offset " + metadata.offset());
        });

        producer.flush();
    }

}
