package kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class DataConsumer {

    static String KAFKA_BROKERS = "localhost:9092";
    static String TOPIC_NAME="data";
    static String GROUP_ID="group";

    public static Consumer createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        return consumer;
    }

    public static String runConsumer(Consumer consumer) {

        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

        if (consumerRecords.count() == 0)
            return "";

        String priceList = "";
        for (ConsumerRecord r : consumerRecords) {
            String symbol = r.key().toString();
            if (symbol.charAt(0) != '[')
                    return "";
            for (int i=1; i< symbol.length(); i++){
                char c = symbol.charAt(i);
                if (c == ']'){
                    priceList += ',';
                    break;
                }
                priceList += c;
            }
            priceList += r.value();
            priceList += ";";
        }
        priceList = priceList.replace('\n', ',');

//        consumerRecords.forEach(record -> {
//            System.out.println("Number of records: " + consumerRecords.count());
//            System.out.println("Record Key:\n" + record.key() + "\nRecord value:\n" + record.value());
//            System.out.println("Record partition " + record.partition() + " Record offset " + record.offset());
//        });

        consumer.commitAsync();
        return priceList;
    }

}
