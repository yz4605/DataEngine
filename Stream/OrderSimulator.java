package kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class OrderSimulator {

    public static void runProducer(Producer producer, String topic, String ticker, int low, int high, long order) {
        //final float fixed = (float) ThreadLocalRandom.current().nextInt(low,high) / 20;
        IntStream percent = ThreadLocalRandom.current().ints(low, high).limit(order);
        percent.forEach(x -> {
            float num = 100 + (float) x / 1000;
            //num += fixed;
            String price = String.format("%.4f%n", num);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, '['+ticker+']', price);
            System.out.println('['+ticker+']' + price);
            producer.send(record);
            //RecordMetadata metadata = producer.send(record).get();
            //System.out.println("Record sent with key " + index + " to partition " + metadata.partition() + " with offset " + metadata.offset());
        });
        producer.flush();
    }

    public static String readSymbol(String path) throws IOException {
        try (BufferedReader br =
                     new BufferedReader(new FileReader(path))) {
            return br.readLine();
        }
    }

    public static void startTrade(String path) {
        int volume = 0;
        Producer<String, String> producer = DataProducer.createProducer("localhost:9092","simulator");
        try {
            String symbols = readSymbol(path);
            String[] stockList = symbols.split(",");
            while(true)
            {
                for (String s : stockList) {
                    int vol = ThreadLocalRandom.current().nextInt(101);
                    runProducer(producer,"order" ,s,-100,101,(long)vol);
                    volume += vol;
                }
                System.out.println("Volume: "+volume);
                volume = 0;
            }
        }
        catch (Exception e){
            System.out.println("Read Stock Symbol Error");
        }
        finally {
            producer.close();
        }
    }

}
