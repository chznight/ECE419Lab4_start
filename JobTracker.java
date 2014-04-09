import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import java.util.*; 
import java.net.*;
import java.io.*;
import java.util.List;

import java.io.IOException;

public class JobTracker {
    
    static String squenceNumDispenser = "/squenceNumDispenser";
    static String myTasks = "/tasks";
    static String workerIdDispenser = "/workerIdDispenser";
    static String jobTrackerBoss = "/jobTrackerBoss";
    static String availableWorkers = "/availableWorkers";
    

    public static void main(String[] args) {

        ServerSocket serverSocket = null;
        boolean listening = true;
        int myPort;

        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker {zkServer:clientPort} {myServerPort}");
            return;
        }

        ZkConnector zkc = new ZkConnector();
        myPort = Integer.parseInt(args[1]);

        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        try {
            serverSocket = new ServerSocket(myPort);
        } catch (IOException e) {
            System.err.println("ERROR: Could not listen on port!");
            System.exit(-1);
        }

        ZooKeeper zk = zkc.getZooKeeper();

        try {

            //path of squence number, later for error handling
            System.out.println("Creating " + squenceNumDispenser);
            Stat s = zk.exists(squenceNumDispenser, false);
            if (s == null) {
                zk.create(
                    squenceNumDispenser,         // Path of znode
                    null,           // Data not needed.
                    Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                    CreateMode.PERSISTENT   // Znode type, set to Persistent.
                    );
            } else {
                System.out.println("Already Exists: " + squenceNumDispenser);
            }

            System.out.println("Creating " + myTasks);
            s = zk.exists(myTasks, false);
            if (s == null) {
                zk.create(
                    myTasks,         // Path of znode
                    null,           // Data not needed.
                    Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                    CreateMode.PERSISTENT   // Znode type, set to Persistent.
                    );
            } else {
                System.out.println("Already Exists: " + myTasks);
            }

            System.out.println("Creating " + workerIdDispenser);
            s = zk.exists(workerIdDispenser, false);
            if (s == null) {            
                zk.create(
                    workerIdDispenser,         // Path of znode
                    null,           // Data not needed.
                    Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                    CreateMode.PERSISTENT   // Znode type, set to Persistent.
                    );
            } else {
                System.out.println("Already Exists: " + workerIdDispenser);
            }


            System.out.println("Creating " + availableWorkers);
            s = zk.exists(availableWorkers, false);
            if (s == null) {    
                zk.create(
                    availableWorkers,         // Path of znode
                    null,           // Data not needed.
                    Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                    CreateMode.PERSISTENT   // Znode type, set to Persistent.
                    );
            } else {
                System.out.println("Already Exists: " + availableWorkers);
            }

            String hostIpPort = InetAddress.getLocalHost().getHostAddress() + ":" + myPort;
            System.out.println("Creating " + jobTrackerBoss);
            zk.create(
                jobTrackerBoss,         // Path of znode
                hostIpPort.getBytes(),           // Data not needed.
                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                CreateMode.EPHEMERAL   // Znode type, set to Persistent.
                );

            //Here we start to look at the list of availible works
            //If a worker is aviliable, we place a task in his work folder: /tasks/work#/
            //everyone who is avilible has a task placed in his folder
            List<String> list = zk.getChildren(availableWorkers, true);
            while (list.size() == 0) {
                Thread.sleep (5000);
                list = zk.getChildren(availableWorkers, true);
            }

            String workerBee;
            for (int i = 0; i < list.size(); i++) {
                System.out.println (list.get(i));
                workerBee = list.get(i);
                Stat stat = zk.setData (squenceNumDispenser, "nothing".getBytes(), -1);
                //create a task with a unique task number, we will remember this number later for checking if tasks are finished
                zk.create(
                    myTasks + "/" + workerBee + "/task" + stat.getVersion(),
                    null,           // Data not needed.
                    Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                    CreateMode.PERSISTENT   // Znode type, set to Persistent.
                    );
            }

        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
        }

        while (listening) {
            new JobTrackerHandlerThread(serverSocket.accept(), args[0]).start();
        }

        serverSocket.close();
    }
}
