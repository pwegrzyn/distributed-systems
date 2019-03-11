import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class MulticastLogger {

    public static void main(String[] args) {

        MulticastSocket socket = null;
        InetAddress group = null;
        Logger logger = Logger.getLogger("token");  
        FileHandler fh = null;  
        try {
            byte[] buffer = new byte[128];
            socket = new MulticastSocket(29420);
            group = InetAddress.getByName("225.0.0.37");
            socket.joinGroup(group);
            System.out.println("Logging...");
            fh = new FileHandler("token.log");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();  
            fh.setFormatter(formatter); 
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String received = new String(packet.getData(), "UTF-8");
                logger.info(received);
            }
        } catch (Exception e) {
            System.out.println("Ups! Something went wrong :(");
        } finally {
            socket.close();
        }

    }
 
}