import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.KeeperException.Code;

import java.util.*; 
import java.net.*;
import java.io.*;
import java.util.List;

import java.io.IOException;

public class FileServer {
    
    static String availableWorkers = "/availableWorkers";
    static String fileServerBoss = "/fileServerBoss";
    static ZooKeeper zk;
    static ZkConnector zkc;
    static Watcher watcher;
    static int myPort;
    static CountDownLatch nodeCreatedSignal = new CountDownLatch(1);
    
    public static void main(String[] args) {
        
		ArrayList<ArrayList<String>> partition_list=null;
		ServerSocket serverSocket = null;
	    boolean listening = true;

	    if (args.length != 3) {
	    	System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer zkServer:clientPort <Port to be used> <File name of dictionary>");
	    	return;
	    }

	    //load dictionary first
		try{
			ArrayList<String> dictionary_list = new ArrayList<String>();
			FileReader dictionary_file_reader = new FileReader(args[2]);
			BufferedReader reader = new BufferedReader(dictionary_file_reader);
			String line;
			line = reader.readLine();
			int lol=0;
			while (line != null) {
				dictionary_list.add(line);
		        	line = reader.readLine();
		        }
		        reader.close();

			partition_list = new ArrayList<ArrayList<String>>();
			int num_partition=(dictionary_list.size()/1000)+(dictionary_list.size()-((dictionary_list.size()/1000)*1000)==0?0:1);
			int fromIndex=0, toIndex=1000;

			for(int i=0;i<num_partition;i++){
				ArrayList<String> derp=new ArrayList<String>(dictionary_list.subList(fromIndex, toIndex));
				partition_list.add(derp);
				if(i==num_partition-2){
					fromIndex+=1000;
					toIndex=dictionary_list.size();
				}else{
					fromIndex+=1000;
					toIndex+=1000;
				}
			}

			dictionary_list=null; // to save memory
			//yo, no point, memory is still referenced by partition_list
	        
	    } catch(Exception e) {
	        System.out.println(e.getMessage());
	    }


	    zkc = new ZkConnector();
		myPort=Integer.parseInt(args[1]);
		try {
	    	zkc.connect(args[0]);
	    } catch(Exception e) {
	    	System.out.println("Zookeeper connect "+ e.getMessage());
	    }
	    zk = zkc.getZooKeeper();

        watcher = new Watcher() { // Anonymous Watcher
	                            @Override
	                            public void process(WatchedEvent event) {
	                                handleEvent(event);
	                            } 
         					};
         					
        System.out.println("Waiting to replace other file server");
        checkpath();
        try{       
            nodeCreatedSignal.await();
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
	    //-----------------------------------------------------
	    //When take over happens
	    //-----------------------------------------------------
		try {
			serverSocket = new ServerSocket(myPort);
		} catch (IOException e) {
			System.err.println("ERROR: Could not listen on port!");
			System.exit(-1);
		}

		try {
			while (listening) {
				new FileServerHandlerThread(serverSocket.accept(), args[0], partition_list).start();
			}
		} catch (IOException e) {
			System.err.println(e);
		}
			
		try {
	        	serverSocket.close();
		} catch (IOException e) {
			System.err.println(e);
		}
    }

    private static void checkpath() {
        try {
            Stat stat = zkc.exists(fileServerBoss, watcher);
            if (stat == null) {              // znode doesn't exist; let's try creating it
                System.out.println("Creating " + fileServerBoss);
                String hostIpPort = InetAddress.getLocalHost().getHostAddress() + ":" + myPort;
                Code ret = zkc.create(
                    fileServerBoss,         // Path of znode
                    hostIpPort,           // Data not needed.
                    CreateMode.EPHEMERAL   // Znode type
                    );
                if (ret == Code.OK) {
                    nodeCreatedSignal.countDown();
                    System.out.println("I am the File Server Boss!");
                }
            }         
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }

    }

    private static void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(fileServerBoss)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(fileServerBoss + " deleted! Let's go!");       
                checkpath(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(fileServerBoss + " created!");       
                try{ Thread.sleep(5000); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }
}
