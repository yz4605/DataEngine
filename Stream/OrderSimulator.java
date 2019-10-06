package stream;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.concurrent.TimeUnit;

public class OrderSimulator {

    public static void runProducer(Producer producer, String topic, String ticker, int low, int high, long order) {
        //send random price orders to the broker
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        String timeStamp = dtf.format(now);
        IntStream percent = ThreadLocalRandom.current().ints(low, high).limit(order);
        percent.forEach(x -> {
            float num = 100 + (float) x / 1000;
            String price = String.format("%.4f@%s%n", num,timeStamp);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, '['+ticker+']', price);
            producer.send(record);
            //RecordMetadata metadata = producer.send(record).get();
        });
        //send delayed message
        //ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, '['+ticker+']', "90.0@2019-10-01 00:00:00\n");
        //producer.send(record);
        producer.flush();
    }

    public static String readSymbol(String path) throws IOException {
        try (BufferedReader br =
                     new BufferedReader(new FileReader(path))) {
            return br.readLine();
        }
    }

    public static void startTrade(String broker) {
        //simulate trade orders
        int volume = 0;
        int count = 0;
        Producer<String, String> producer = DataProducer.createProducer(broker,"orders");
        Producer<String, String> monitor = DataProducer.createProducer(broker,"volume");
        try {
            String symbols = readSymbol("stock.in");
            String[] stockList = symbols.split(",");
            while(true)
            {
                for (String s : stockList) {
                    int vol = ThreadLocalRandom.current().nextInt(5);
                    runProducer(producer,"order" ,s,-100,101,(long)vol);
                    volume += vol;
                }
                if (count++ > 20){
                    System.out.println("Volume: "+volume);
                    String v = Integer.toString(volume);
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>("monitor", "volume", v);
                    monitor.send(record);
                    monitor.flush();
                    volume = 0;
                    count = 0;
                    TimeUnit.SECONDS.sleep(1);
                }
            }
        }
        catch (Exception e){
            System.out.println(e);
        }
        finally {
            producer.close();
        }
    }

}
