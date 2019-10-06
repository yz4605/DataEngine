package stream;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.HashMap;

public class TrendStock {

    public HashMap<String, float[]> trendList = new HashMap<>(); //trending list
    public int limit=5; //number of price records

    public void filter() {
        //only keep trending stocks
        HashMap<String, float[]> h = this.trendList;
        String[] removeList = new String[h.size()];
        int idx = 0;
        for (String i : h.keySet()) {
            float[] price = h.get(i);
            if (price[price.length-1] == 0F)
                continue;
            //only if more than 5% difference and trends continue
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
        //update trending list with new data
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
        if (trending.length() < 1) {
            return;
        }
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("trending", trending);
        producer.send(record);
        producer.flush();
        System.out.println(trending);
    }

}
