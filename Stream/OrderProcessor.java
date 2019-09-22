package kafka;

import org.apache.kafka.clients.consumer.Consumer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class OrderProcessor {

    public static void writePrice(String path, String content) throws IOException {
        try (BufferedWriter br =
                     new BufferedWriter(new FileWriter(path,true))) {
            br.write(content);
        }
    }

    public static void calculatePrice(String path) {
        Consumer consumer = DataConsumer.createConsumer();
        try {
            while(true)
            {
                String priceList= DataConsumer.runConsumer(consumer);
                if (priceList.length() < 1)
                {
                    continue;
                }

                String[] stock = priceList.split(";");
                for (String s : stock){
                    String[] p = s.split(",");
                    double price = 0;
                    int num = p.length-1;
                    for (int i=1; i<p.length; i++){
                        price += Double.parseDouble(p[i]);
                    }
                    price = price / num;
                    System.out.println("Symbol: "+p[0]);
                    System.out.println("Price: "+price);
                    writePrice(path,p[0]+","+price+"\n");
                }
            }
        }
        catch (Exception e){
            System.out.println("Write Stock Price Error");
        }
        finally {
            consumer.close();
        }
    }

}
