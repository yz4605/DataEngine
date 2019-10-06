package stream;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;

public class OrderCorrect {

    public HashMap<String, Float> priceList = new HashMap<>(); //accurate price
    public HashMap<String, Float> outList = new HashMap<>(); //delayed message

    public String checkOrder (String symbol, String order, Producer correct){
        //check timestamp among the orders
        String[] orders = order.split("\n");
        String result = new String();
        String benchmark1 = orders[0].split("@")[1].split(":")[1];
        String benchmark2 = orders[orders.length-1].split("@")[1].split(":")[1];
        Integer t1 = Integer.parseInt(benchmark1);
        Integer t2 = Integer.parseInt(benchmark2);
        Integer val1 = Math.max(t1,t2);
        for (String s : orders){
            String[] ss = s.split("@");
            String[] val2 = ss[1].split(":");
            if (val1 == 59){
                result += ss[0]+"\n";
                continue;
            }
            if (Math.abs(val1-Integer.parseInt(val2[1])) > 1) {
                if (this.priceList.get(symbol) == null){
                    this.outList.put(symbol, Float.parseFloat(ss[0]));
                    continue;
                }
                if (Math.abs(Float.parseFloat(ss[0])-this.priceList.get(symbol)) > 1){
                    //only if more thank 1% difference
                    this.outList.put(symbol, Float.parseFloat(ss[0]));
                    continue;
                }
            }
            result += ss[0]+"\n";
        }
        String monitor = new String();
        for (String i : this.outList.keySet()) {
            monitor += i+","+this.outList.get(i)+";";
        }
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("monitor", "delay", monitor);
        correct.send(record);
        correct.flush();
        this.outList.clear();
        return result;
    }

}
