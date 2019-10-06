package stream;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

public class OrderProcessor {

    public static String runConsumer(Consumer consumer, OrderCorrect check, Producer correct) {
        //validate trade orders
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
        if (consumerRecords.count() == 0)
            return "";
        String priceList = "";
        for (ConsumerRecord r : consumerRecords) {
            String symbol = r.key().toString();
            if (symbol.charAt(0) != '[')
                return "";
            String cleanSymbol = new String();
            for (int i=1; i < symbol.length(); i++) {
                char c = symbol.charAt(i);
                if (c == ']') {
                    break;
                }
                cleanSymbol += c;
            }
            priceList += cleanSymbol+",";
            String price = check.checkOrder(cleanSymbol, r.value().toString(),correct);
            priceList += price;
            priceList += ";";
        }
        priceList = priceList.replace('\n', ',');
        consumer.commitSync();
        return priceList;
    }

    public static void writePrice(String path, String content) throws IOException {
        try (BufferedWriter br =
                     new BufferedWriter(new FileWriter(path,true))) {
            br.write(content);
        }
    }

    public static void calculatePrice(String broker) {
        //calculate stock price from trade orders
        Producer<String, String> producer = DataProducer.createProducer(broker,"trends");;
        Producer<String, String> correct = DataProducer.createProducer(broker,"correct");
        Consumer consumer = DataConsumer.createConsumer(broker,"price","group");
        TrendStock t = new TrendStock();
        OrderCorrect check = new OrderCorrect();
        try {
            while(true)
            {
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                LocalDateTime now = LocalDateTime.now();
                String timeStamp = dtf.format(now);
                String content = "";
                String streamContent = "";
                String priceList= runConsumer(consumer,check,correct);
                if (priceList.length() < 1)
                    continue;
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
                    check.priceList.put(p[0],price);
                }
                t.run(producer,streamContent);
                writePrice("price.out",content);
            }
        }
        catch (Exception e){
            System.out.println(e);
        }
        finally {
            producer.close();
            consumer.close();
        }
    }
}
