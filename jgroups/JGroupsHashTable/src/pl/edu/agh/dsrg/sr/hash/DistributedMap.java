package pl.edu.agh.dsrg.sr.hash;

import com.google.protobuf.InvalidProtocolBufferException;
import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class DistributedMap extends ReceiverAdapter implements SimpleStringMap {

    private final JChannel channel;
    private ConcurrentMap<String,Integer> map;

    public DistributedMap(String clusterName) throws Exception {
        this.map = new ConcurrentHashMap<>();
        this.channel = new JChannel(false);
        ProtocolStack stack = new ProtocolStack();
        this.channel.setProtocolStack(stack);
        stack.addProtocol(new UDP()
                        .setValue("mcast_group_addr", InetAddress.getByName("230.100.200.179")))
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL()
                        .setValue("timeout", 12000)
                        .setValue("interval", 3000))
                .addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2())
                .addProtocol(new STATE_TRANSFER());
        stack.init();
        this.channel.setDiscardOwnMessages(true);
        this.channel.setReceiver(this);
        this.channel.connect(clusterName);
        this.channel.getState(null, 10000);
    }

    /* -------------- Interface methods --------------------- */

    @Override
    public boolean containsKey(String key) {
        return this.map.containsKey(key);
    }

    @Override
    public Integer get(String key) {
        return this.map.get(key);
    }

    @Override
    public void put(String key, Integer value) {
        this.map.put(key, value);
        sendUpdate(HashMapUpdateProtos.HashMapUpdate.OperationType.PUT, key, value);
    }

    @Override
    public Integer remove(String key) {
        sendUpdate(HashMapUpdateProtos.HashMapUpdate.OperationType.REMOVE, key, null);
        return this.map.remove(key);
    }

    public void printMap() {
        System.out.println("----------- CURRENT MAP CONTENT -----------");
        for(Map.Entry<String,Integer> entry : this.map.entrySet()) {
            System.out.println("KEY: " + entry.getKey() + " | VALUE: " + entry.getValue());
        }
        System.out.println("-------------------------------------------");
    }

    /* ----------------- Channel methods -------------------- */

    public void finish() {
        Util.close(this.channel);
    }

    /* ------------ State transfer methods ------------------- */

    @Override
    public void getState(OutputStream outputStream) throws Exception {
        HashMap<String,Integer> copy = new HashMap<>();
        for(Map.Entry<String,Integer> entry : this.map.entrySet()) {
            copy.put(entry.getKey(), entry.getValue());
        }
        try(ObjectOutputStream objectStream = new ObjectOutputStream(new BufferedOutputStream(outputStream, 1024))) {
            objectStream.writeObject(copy);
        }
    }

    @Override
    public void setState(InputStream inputStream) throws Exception {
        HashMap<String,Integer> newMap;
        try(ObjectInputStream objectStream = new ObjectInputStream(inputStream)) {
            newMap = (HashMap<String, Integer>)objectStream.readObject();
        }
        if(newMap != null) {
            this.map.clear();
            this.map.putAll(newMap);
        }
    }

    @Override
    public void receive(Message msg) {
        HashMapUpdateProtos.HashMapUpdate update = null;
        try {
            update = HashMapUpdateProtos.HashMapUpdate.parseFrom(msg.getBuffer());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        if(update == null) return;
        handleUpdate(update);
    }

    @Override
    public void viewAccepted(View newView) {
        handleNewView(this.channel, newView);
    }

    private void handleNewView(JChannel channel, View newView) {
        if(newView instanceof MergeView) {
            ViewHandler handler = new ViewHandler(channel, (MergeView)newView);
            handler.start();
        }
    }

    private static class ViewHandler extends Thread {

        private JChannel channel;
        private MergeView view;

        private ViewHandler(JChannel channel, MergeView view) {
            this.channel = channel;
            this.view = view;
        }

        public void run() {
            List<View> subgroups = this.view.getSubgroups();
            View tmpView = subgroups.get(0);
            Address localAddress = this.channel.getAddress();
            if (!tmpView.getMembers().contains(localAddress)) {
                System.out.println("Re-acquiring state...");
                try {
                    this.channel.getState(null, 10000);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                System.out.println("Done.");
            } else {
                System.out.println("In primary partition. Skipping state re-acquiring.");
            }
        }
    }

    private void sendUpdate(HashMapUpdateProtos.HashMapUpdate.OperationType type, String key, Integer value) {
        HashMapUpdateProtos.HashMapUpdate putUpdate;
        if (value != null) {
            putUpdate = HashMapUpdateProtos.HashMapUpdate
                    .newBuilder()
                    .setType(type)
                    .setKey(key)
                    .setValue((double)value)
                    .build();
        } else {
            putUpdate = HashMapUpdateProtos.HashMapUpdate
                    .newBuilder()
                    .setType(type)
                    .setKey(key)
                    .build();
        }
        byte[] buffer = putUpdate.toByteArray();
        try {
            this.channel.send(new Message(null, buffer));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleUpdate(HashMapUpdateProtos.HashMapUpdate update) {
        if(update.getType() == HashMapUpdateProtos.HashMapUpdate.OperationType.PUT) {
            this.map.put(update.getKey(), (int)update.getValue());
        } else if(update.getType() == HashMapUpdateProtos.HashMapUpdate.OperationType.REMOVE) {
            this.map.remove(update.getKey());
        }
    }

    /* ----------------------------------------------------------- */
}
