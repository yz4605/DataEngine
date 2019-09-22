package kafka;

import org.apache.kafka.clients.producer.Producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class OrderSimulator {

    public static String readSymbol(String path) throws IOException {
        try (BufferedReader br =
                     new BufferedReader(new FileReader(path))) {
            return br.readLine();
        }
    }

    public static void startTrade(String path) {
        Producer<String, String> producer = DataProducer.createProducer();
        try {
            String symbols = readSymbol(path);
            String[] stockList = symbols.split(",");
            while(true)
            {
                for (String s : stockList) {
                    DataProducer.runProducer(producer, s,-1000,1001,10L);
                }
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
