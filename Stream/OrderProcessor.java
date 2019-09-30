package kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

public class OrderProcessor {

    public static String runConsumer(Consumer consumer) {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(500));
        if (consumerRecords.count() == 0)
            return "";
        String priceList = "";
        for (ConsumerRecord r : consumerRecords) {
            String symbol = r.key().toString();
            if (symbol.charAt(0) != '[')
                return "";
            for (int i=1; i < symbol.length(); i++) {
                char c = symbol.charAt(i);
                if (c == ']') {
                    priceList += ',';
                    break;
                }
                priceList += c;
            }
            priceList += r.value().toString();
            priceList += ";";
        }
        priceList = priceList.replace('\n', ',');

//        consumerRecords.forEach(record -> {
//            System.out.println("Number of records: " + consumerRecords.count());
//            System.out.println("Record Key:\n" + record.key() + "\nRecord value:\n" + record.value());
//            System.out.println("Record partition " + record.partition() + " Record offset " + record.offset());
//        });

        consumer.commitSync();
        return priceList;
    }

    public static void writePrice(String path, String content) throws IOException {
        try (BufferedWriter br =
                     new BufferedWriter(new FileWriter(path,true))) {
            br.write(content);
        }
    }

    public static void calculatePrice(String path) {
        Producer<String, String> producer = DataProducer.createProducer("localhost:9092","filter");;
        Consumer consumer = DataConsumer.createConsumer("localhost:9092","price","group");
        TrendStock t = new TrendStock();
        try {
            while(true)
            {
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                LocalDateTime now = LocalDateTime.now();
                String timeStamp = dtf.format(now);
                System.out.println(timeStamp);
                String content = "";
                String streamContent = "";
                String priceList= runConsumer(consumer);
                if (priceList.length() < 1) continue;
                String[] stocks = priceList.split(";");
                for (String s : stocks){
                    final float fixed = (float) ThreadLocalRandom.current().nextInt(-100,101) / 20;
                    String[] p = s.split(",");
                    int num = p.length-1;
                    float[] log = new float[num];
                    float price = 0;
                    for (int i=1; i<p.length; i++){
                        float f = Float.parseFloat(p[i]);;
                        price += f;
                        log[i-1] = f;
                    }
                    price = price / num;
                    price += fixed;
                    content += (timeStamp+","+p[0]+","+price+"\n");
                    streamContent += (p[0]+","+price+","+num+"\n");
                }
                t.run(producer,streamContent);
                writePrice(path,content);
                //System.out.println(content);
            }
        }
        catch (Exception e){
            System.out.println("Write Stock Price Error"+e);
        }
        finally {
            producer.close();
            consumer.close();
        }
    }
}
