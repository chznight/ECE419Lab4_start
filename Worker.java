import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;
import java.util.*; 
import java.net.*;
import java.io.*;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
import java.util.concurrent.CountDownLatch;

public class Worker {
    
    static String squenceNumDispenser = "/squenceNumDispenser";
    static String myTasks = "/tasks";
    static String workerIdDispenser = "/workerIdDispenser";
    static String jobTrackerBoss = "/jobTrackerBoss";
    static String availableWorkers = "/availableWorkers";
    static String inProgress = "/inProgress";
    static String finishedJobs = "/finishedJobs";  
    static String tempResults = "/tempResults";  
    static String fileServerBoss = "/fileServerBoss";

    static ZooKeeper zk;
    static ZkConnector zkc;
    static Watcher watcher;
    static CountDownLatch nodeCreatedSignal;

    static Socket socket = null;
    static ObjectOutputStream toServer;
    static ObjectInputStream fromServer;

    public static String getHash(String word) {

        String hash = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            BigInteger hashint = new BigInteger(1, md5.digest(word.getBytes()));
            hash = hashint.toString(16);
            while (hash.length() < 32) hash = "0" + hash;
        } catch (NoSuchAlgorithmException nsae) {
            // ignore
        }
        return hash;
    }

    public static void main(String[] args) {
  
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:clientPort");
            return;
        }

        watcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };   

        nodeCreatedSignal = new CountDownLatch(1);
        zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        int myWorkerId;
        zk = zkc.getZooKeeper();
        
        String myWorkFolder = "";
        boolean running = true;

        try {
            Stat stat = zk.exists(jobTrackerBoss, false);
            while (stat == null) {
                Thread.sleep(5000);
                stat = zk.exists(jobTrackerBoss, false);
            }
            Thread.sleep(1000);
            stat = zk.setData (workerIdDispenser, "nothing".getBytes(), -1);
            System.out.println ("version: " + stat.getVersion());
            myWorkerId = stat.getVersion();
            
            //make myself seen in the available workers dicrectory
            //this is temperary node, exist only when im online
            zk.create(
                availableWorkers + "/worker" + stat.getVersion(),// Path of znode
                null,           // Data not needed.
                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                CreateMode.EPHEMERAL  
                );

            //create task folder for myself
            myWorkFolder = myTasks + "/worker" + stat.getVersion();
            zk.create(
                myWorkFolder,// Path of znode
                null,           // Data not needed.
                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                CreateMode.PERSISTENT   // Znode type, set to Persistent.
                );
        } catch (KeeperException e) {
            System.out.println(e.code());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        System.out.println ("Waiting for file server");

        checkpath();
        DictionaryPacket packetToServer;
        DictionaryPacket packetFromServer;
        while (running == true) {
            try {
                nodeCreatedSignal.await();
                //look for tasks and delete them
                List<String> list = zk.getChildren(myWorkFolder, true);
                while (list.size() == 0) {
                    Thread.sleep (5000);
                    list = zk.getChildren(myWorkFolder, true);
                }

                for (int i = 0; i < list.size(); i++) {
                    System.out.println (list.get(i));
                    //create a task with a unique task number, we will remember this number later for checking if tasks are finished
                    //process here... then delete
                    byte[] data = zk.getData(myWorkFolder+"/"+list.get(i), false, null);
                    String dataString = new String(data);

                    //token 0 is fromIndex, token 1 is toIndex, token 2 is hash
                    String[] dataStringToken = dataString.split ("[ ]+");
                    int fromIndex = Integer.parseInt(dataStringToken[0]);
                    int toIndex = Integer.parseInt(dataStringToken[1]);
                    String hashKey = dataStringToken[2];
                    String password = "";
                    boolean found_key = false;

                    for (int index_counter = fromIndex; index_counter < toIndex; index_counter++) {
                        if (found_key == true) {
                            break;
                        }
                        packetToServer = new DictionaryPacket();
                        packetToServer.type = DictionaryPacket.DICT_REQUEST;
                        packetToServer.index = index_counter;
                        toServer.writeObject(packetToServer);
                        packetFromServer = (DictionaryPacket) fromServer.readObject();
                        if (packetFromServer.type != DictionaryPacket.DICT_REPLY) {
                            System.out.println ("unrecognized packet");
                        }
                        int chunk_size = packetFromServer.content.size();
                        for (int word_counter = 0; word_counter < chunk_size; word_counter++) {
                            String temp_word = packetFromServer.content.get(word_counter);
                            String temp_hash = getHash(temp_word);
                            if (temp_hash.equals(hashKey)) {
                                found_key = true;
                                password = temp_word;
                                break;
                            }
                        }
                    }

                    if (found_key == true) {
                        zk.create (tempResults + "/" + list.get(i), password.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } else {
                        zk.create (tempResults + "/" + list.get(i), "NOT_FOUND".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                    zk.delete(myWorkFolder + "/" + list.get(i), 0);

                    //need to clean up here
                    int temp_tasks_done = 0;
                    found_key = false;
                    List<String> temp_results = zk.getChildren(tempResults, true);
                    data = zk.getData(inProgress+"/"+hashKey, false, null);
                    dataString = new String(data);
                    dataStringToken = dataString.split (":");
                    String[] allTasks = dataStringToken[1].split("[ ]+");
                    for (int j = 0; j < allTasks.length; j++) {
                        for (int k = 0; k < temp_results.size(); k++) {
                            if (temp_results.get(k).equals("task"+allTasks[j])) {
                                temp_tasks_done++;
                                byte[] password_data = zk.getData (tempResults + "/" + temp_results.get(k), false, null);
                                String password_string = new String (password_data);
                                if (password_string.equals("NOT_FOUND") == false) {
                                    found_key = true;
                                    password = password_string;
                                }
                            }
                        }
                    }
                    if (temp_tasks_done == allTasks.length) {
                        //all nodes are done, time to delete
                        if (found_key == true)
                            zk.create (finishedJobs + "/" + hashKey, password.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        else
                            zk.create (finishedJobs + "/" + hashKey, "NOT_FOUND".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                        zk.delete (inProgress + "/" + hashKey, 0);
                        for (int i2 = 0; i2 < allTasks.length; i2++) {
                            zk.delete (tempResults + "/task" + allTasks[i2], 0);
                        }
                    }
                }
            } catch (IOException e) {
                System.out.println ("File server boss went down");
                System.out.println(e.getMessage());
                try {
                	Thread.sleep(5000);
                } catch (Exception e2) {
                
                }
                continue;
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

    }

    public static void checkpath() {
        try {
            Stat stat = zkc.exists(fileServerBoss, watcher);
            if (stat != null) {
                byte[] data = zk.getData (fileServerBoss, false, null);
                String dataString = new String (data);
                String[] ip_port = dataString.split(":");
                int server_port = Integer.parseInt(ip_port[1]);
                socket = new Socket (ip_port[0], server_port);
                toServer = new ObjectOutputStream(socket.getOutputStream());
                fromServer = new ObjectInputStream(socket.getInputStream());
                nodeCreatedSignal.countDown();
                System.out.println("Connected to fileServer");
            }         
        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println ("cant connect to host");
            System.out.println(e.getMessage());
        }
    }

    private static void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(fileServerBoss)) {
            if (type == EventType.NodeDeleted) {
                nodeCreatedSignal = new CountDownLatch(1);
                System.out.println(fileServerBoss + " down");
            }
            if (type == EventType.NodeCreated) {
                System.out.println(fileServerBoss + " back up, will reconnect");
            }
            checkpath(); // re-enable the watch
            
        }
    }

}
