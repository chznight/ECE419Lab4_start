import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.io.IOException;


public class Worker {
    
    static CountDownLatch nodeCreatedSignal = new CountDownLatch(1);
    static String squenceNumDispenser = "/squenceNumDispenser";
    static String myTasks = "/tasks";
    static String workerIdDispenser = "/workerIdDispenser";
    static String jobTrackerBoss = "/jobTrackerBoss";
    static String availableWorkers = "/availableWorkers";


    public static void main(String[] args) {
  
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:clientPort");
            return;
        }
    
        ZkConnector zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        int myWorkerId;
        ZooKeeper zk = zkc.getZooKeeper();
        
        // try {
        //     zk.exists(
        //         myPath, 
        //         new Watcher() {       // Anonymous Watcher
        //             @Override
        //             public void process(WatchedEvent event) {
        //                 // check for event type NodeCreated
        //                 boolean isNodeCreated = event.getType().equals(EventType.NodeCreated);
        //                 // verify if this is the defined znode
        //                 boolean isMyPath = event.getPath().equals(myPath);
        //                 if (isNodeCreated && isMyPath) {
        //                     System.out.println(myPath + " created!");
        //                     nodeCreatedSignal.countDown();
        //                 }
        //             }
        //         });
        // } catch(KeeperException e) {
        //     System.out.println(e.code());
        // } catch(Exception e) {
        //     System.out.println(e.getMessage());
        // }
                            
        // System.out.println("Waiting for " + myPath + " to be created ...");
        
        // try{       
        //     nodeCreatedSignal.await();
        // } catch(Exception e) {
        //     System.out.println(e.getMessage());
        // }

        // System.out.println("DONE");
        String myWorkFolder;
        boolean running = true;

        try {
            Stat stat = zk.setData (workerIdDispenser, "nothing".getBytes(), -1);
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


            while (running == true) {
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
                    zk.delete(myWorkFolder + "/" + list.get(i), 0);
                }
            }
        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }


    }
}
