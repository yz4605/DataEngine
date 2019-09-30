package kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.HashMap;

public class TrendStock {

    public HashMap<String, float[]> trendList = new HashMap<>();
    public int limit=5;

    public void filter() {
        HashMap<String, float[]> h = this.trendList;
        String[] removeList = new String[h.size()];
        int idx = 0;
        for (String i : h.keySet()) {
            float[] price = h.get(i);
            if (price[price.length-1] == 0F)
                continue;
            if (price[price.length - 1] - price[1] > 5) {
                if (price[price.length - 1] > price[price.length - 2]) {
                    continue;
                }
            } else if(price[price.length - 1] - price[1] < -5) {
                if (price[price.length - 1] < price[price.length - 2]){
                    continue;
                }
            }

            removeList[idx++] = i;

        }
        for (int k = 0; k<idx; k++) {
            h.remove(removeList[k]);
        }

    }

    public void update(String symbol, float price, int vol) {
        HashMap<String, float[]> h = this.trendList;
        if (h.containsKey(symbol)) {
            int flag = 0;
            float[] p = h.get(symbol);
            p[0] += vol;
            for (int i=1; i < p.length; i++){
                if (p[i] == 0F) {
                    p[i] = price;
                    flag = 1;
                    break;
                }
            }
            if (flag == 0) {
                for (int i=1; i < p.length-1; i++){
                    p[i] = p[i+1];
                }
                p[p.length-1] = price;
            }
        } else {
            float[] p = new float[this.limit+1];
            Arrays.fill(p,0F);
            p[0] = vol;
            p[1] = price;
            h.put(symbol, p);
        }
//        System.out.println("Update");
//        for (String i : this.trendList.keySet()){
//            System.out.println(i+": "+ Arrays.toString(this.trendList.get(i)));
//        }
        filter();
    }

    public void run (Producer producer, String content) {
        String[] lines = content.split("\n");
        for (String l : lines) {
            String[] s = l.split(",");
            update(s[0],Float.parseFloat(s[1]),Integer.parseInt(s[2]));
        }
        String trending = "";
        for (String i : this.trendList.keySet()){
            float[] f = this.trendList.get(i);
            if (f[f.length-1] == 0F)
                continue;
            trending += i +","+ Arrays.toString(f)+";";
        }
        System.out.println(trending);
        if (trending.length() < 1) {
            return;
        }
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("trend", trending);
        producer.send(record);
        producer.flush();
    }

}
