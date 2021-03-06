import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import java.util.concurrent.CountDownLatch;



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
    static String inProgress = "/inProgress";
    static String finishedJobs = "/finishedJobs";    
    static String tempResults = "/tempResults";  
    static ZooKeeper zk;
    static ZkConnector zkc;
    static Watcher watcher;
    static Watcher watcherReschedule;
    static int myPort;
    static CountDownLatch nodeCreatedSignal = new CountDownLatch(1);

    public static void main(String[] args) {

        ServerSocket serverSocket = null;
        boolean listening = true;

        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker {zkServer:clientPort} {myServerPort}");
            return;
        }

        zkc = new ZkConnector();
        myPort = Integer.parseInt(args[1]);

        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        zk = zkc.getZooKeeper();
        watcher = new Watcher() { // watch for replacement backup
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };

        watcherReschedule = new Watcher() { // watch for replacement backup
                            @Override
                            public void process(WatchedEvent event) {
                                System.out.println (event.getType());
                                boolean isMyPath = event.getPath().equals(availableWorkers);
                                if (isMyPath) {
                                    taskReschedule();
                                }
                            } 
                        };
        checkpath();

        System.out.println ("Waiting for other Job Tracker to go down...");
        try{       
            nodeCreatedSignal.await();
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }

        try {
            serverSocket = new ServerSocket(myPort);
        } catch (IOException e) {
            System.err.println("ERROR: Could not listen on port!");
            System.exit(-1);
        }

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

            System.out.println("Creating " + inProgress);
            s = zk.exists(inProgress, false);
            if (s == null) {    
                zk.create(
                    inProgress,         // Path of znode
                    null,           // Data not needed.
                    Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                    CreateMode.PERSISTENT   // Znode type, set to Persistent.
                    );
            } else {
                System.out.println("Already Exists: " + inProgress);
            }

            System.out.println("Creating " + finishedJobs);
            s = zk.exists(finishedJobs, false);
            if (s == null) {    
                zk.create(
                    finishedJobs,         // Path of znode
                    null,           // Data not needed.
                    Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                    CreateMode.PERSISTENT   // Znode type, set to Persistent.
                    );
            } else {
                System.out.println("Already Exists: " + finishedJobs);
            }

            System.out.println("Creating " + tempResults);
            s = zk.exists(tempResults, false);
            if (s == null) {    
                zk.create(
                    tempResults,         // Path of znode
                    null,           // Data not needed.
                    Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                    CreateMode.PERSISTENT   // Znode type, set to Persistent.
                    );
            } else {
                System.out.println("Already Exists: " + tempResults);
            }



        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
        }

        taskReschedule();
        System.out.println ("starting to listen for clients");
		try {
		    while (listening) {
		        new JobTrackerHandlerThread(serverSocket.accept(), args[0]).start();
		    }
		} catch (IOException e) {

		}
		
		try {
        	serverSocket.close();
		} catch (IOException e) {

		}

    }

    private static void checkpath() {
        try {
            Stat stat = zkc.exists(jobTrackerBoss, watcher);
            if (stat == null) {              // znode doesn't exist; let's try creating it
                System.out.println("Creating " + jobTrackerBoss);
                String hostIpPort = InetAddress.getLocalHost().getHostAddress() + ":" + myPort;
                Code ret = zkc.create(
                    jobTrackerBoss,         // Path of znode
                    hostIpPort,           // Data not needed.
                    CreateMode.EPHEMERAL   // Znode type
                    );
                if (ret == Code.OK) {
                    nodeCreatedSignal.countDown();
                    System.out.println("I am the Job Tracker Boss!");
                }
            }         
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(jobTrackerBoss)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(jobTrackerBoss + " deleted! Let's go!");       
                checkpath(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(jobTrackerBoss + " created!");       
                try{ Thread.sleep(5000); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }

    private static void taskReschedule() {
        try {
            System.out.println ("In Rescheduler");
            int distributor = 0;
            List<String> available_workers = zk.getChildren(availableWorkers, watcherReschedule);
            int worker_list_size = available_workers.size();
            if (available_workers.size() == 0) { //no working node, cant really do anything
                return;
            }
            List<String> curr_and_past_workers = zk.getChildren(myTasks, true);        
            if (curr_and_past_workers.size() == 0) { //no past working node, no need to do anything
                return;
            }
            for (int i = 0; i < curr_and_past_workers.size(); i++) {
                String curr_worker = curr_and_past_workers.get(i);
                boolean found = false;
                for (int j = 0; j < available_workers.size(); j++) {
                    String avail_worker = available_workers.get(j);
                    if (curr_worker.equals(avail_worker)) {
                         //this means this worker is still up, we found him in the availible worker dicrectroy
                        found = true;
                    }
                }
                if (found == false) {
                    //this means this worker is not availible anymore, we need to clean up
                    List<String> tasks = zk.getChildren(myTasks + "/" + curr_worker, true);
                    if (tasks.size() == 0) {
                        //this worker doesnt have any pending tasks, we will delete him
                        zk.delete(myTasks + "/" + curr_worker, 0);
                    } else {
                        //we need to reshedule his tasks
                        for (int k = 0; k < tasks.size(); k++) {
                            byte[] data = zk.getData(myTasks + "/" + curr_worker + "/" + tasks.get(k), false, null);
                            zk.create(
                                myTasks + "/" + available_workers.get((distributor++)%worker_list_size) + "/" + tasks.get(k),
                                data,         
                                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                                CreateMode.PERSISTENT   // Znode type, set to Persistent.
                                );        
                            zk.delete(myTasks + "/" + curr_worker + "/" + tasks.get(k), 0);            
                        }
                        zk.delete(myTasks + "/" + curr_worker, 0);
                    }
                }
            }
        } catch(KeeperException e) {
            System.out.println(e.code());
        } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
        }
    }
}
