package kafka;

import java.util.Scanner;

public class App {

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);
        System.out.println("Enter Command");
        String command = sc.nextLine();
        switch (command) {
            case "1":
                OrderSimulator.startTrade("localhost:9092");
                break;
            case "2":
                OrderProcessor.calculatePrice("localhost:9092");
                break;
            case "3":
                DataStream.runStream("localhost:9092");
                break;
        }
    }

}
