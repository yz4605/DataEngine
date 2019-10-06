package stream;

import java.util.Scanner;

public class App {

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);
        System.out.println("Enter Broker");
        String broker = sc.nextLine();
        if (broker.length()<1){
            broker = "localhost:9092";
        }
        System.out.println("Enter Command");
        String command = sc.nextLine();
        switch (command) {
            case "1":
                OrderSimulator.startTrade(broker);
                break;
            case "2":
                OrderProcessor.calculatePrice(broker);
                break;
            case "3":
                DataStream.runStream(broker);
                break;
        }
    }

}
