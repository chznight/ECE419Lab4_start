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

import java.io.IOException;

public class JobTracker {
    
    static String squenceNumDispenser = "/squenceNumDispenser";
    static String myTasks = "/tasks";
    static String workerIdDispenser = "/workerIdDispenser";
    static String jobTrackerBoss = "/jobTrackerBoss";
    static String availableWorkers = "/availableWorkers";
    

    public static void main(String[] args) {
        
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker zkServer:clientPort");
            return;
        }

        ZkConnector zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        ZooKeeper zk = zkc.getZooKeeper();

        try {
            System.out.println("Sleeping...");
            Thread.sleep(2000);

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

            String hostIpPort = InetAddress.getLocalHost().getHostAddress() + ":1113";
            System.out.println("Creating " + jobTrackerBoss);
            zk.create(
                jobTrackerBoss,         // Path of znode
                hostIpPort.getBytes(),           // Data not needed.
                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                CreateMode.EPHEMERAL   // Znode type, set to Persistent.
                );

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

            Thread.sleep (30000);

        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
        }
    }
}
