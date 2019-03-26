package pl.edu.agh.dsrg.sr.hash;

import org.jgroups.protocols.UDP;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;


public class DemoApp {

    public static void main(String[] args) {

        try {

            System.setProperty("java.net.preferIPv4Stack", "true");
            new UDP().setValue("mcast_group_addr", InetAddress.getByName("230.100.200.179"));
            DistributedMap map = new DistributedMap("hashmaptestnetwork");
            eventLoop(map);
            map.finish();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void eventLoop(DistributedMap map) throws IOException {
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        boolean quit = false;
        String key;
        Integer value;
        while(!quit) {
            System.out.print(">> ");
            String line = input.readLine().toLowerCase();
            switch (line) {
                case "print":
                    map.printMap();
                    break;
                case "put":
                    System.out.print("Please provide the key: "); System.out.flush();
                    key = input.readLine();
                    System.out.print("Please provide the value: "); System.out.flush();
                    value = Integer.parseInt(input.readLine());
                    map.put(key, value);
                    System.out.println("K,V pair successfully added to the map."); System.out.flush();
                    break;
                case "get":
                    System.out.print("Please provide the key: "); System.out.flush();
                    key = input.readLine();
                    value = map.get(key);
                    System.out.println("Value for this key: " + value); System.out.flush();
                    break;
                case "containskey":
                    System.out.print("Please provide the key: "); System.out.flush();
                    key = input.readLine();
                    if(map.containsKey(key)) {
                        System.out.println("The map contains this key."); System.out.flush();
                    } else {
                        System.out.println("This map does not contain this key."); System.out.flush();
                    }
                    break;
                case "remove":
                    System.out.print("Please provide the key: "); System.out.flush();
                    key = input.readLine();
                    if(map.remove(key) == null) {
                        System.out.println("The map did not contain this key."); System.out.flush();
                    } else {
                        System.out.println("K,V pair successfully removed."); System.out.flush();
                    }
                    break;
                case "quit":
                case "exit":
                    quit = true;
                    break;
                default:
                    System.out.println("Unknown command.");
                    break;
            }
        }
    }

}
