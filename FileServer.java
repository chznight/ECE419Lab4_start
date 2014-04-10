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

public class FileServer {
    
    static String availableWorkers = "/availableWorkers";
    static String fileServerBoss = "/fileServerBoss";
    
    public static void main(String[] args) {
        
	ArrayList<ArrayList<String>> partition_list=null;
	ServerSocket serverSocket = null;
        boolean listening = true;
        int myPort;

        if (args.length != 3) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer zkServer:clientPort <Port to be used> <File name of dictionary>");
            return;
        }

        ZkConnector zkc = new ZkConnector();
	myPort=Integer.parseInt(args[1]);
	
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
		int fromIndex=0, toIndex=999;

		for(int i=0;i<num_partition;i++){
			ArrayList<String> derp=new ArrayList<String>(dictionary_list.subList(fromIndex, toIndex));
			partition_list.add(derp);
			if(i==num_partition-2){
				fromIndex+=1000;
				toIndex=dictionary_list.size()-1;
			}else{
				fromIndex+=1000;
				toIndex+=1000;
			}
		}

		dictionary_list=null; // to save memory

        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        ZooKeeper zk = zkc.getZooKeeper(); /*How to find the job tracker ip and port*/

	try{
            String hostIpPort = InetAddress.getLocalHost().getHostAddress() + ":" + myPort;
            System.out.println("Creating " + fileServerBoss);
            zk.create(
                fileServerBoss,         // Path of znode
                hostIpPort.getBytes(),           // Data not needed.
                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                CreateMode.EPHEMERAL   // Znode type, set to Persistent.
                );
	}catch(Exception e){
		System.err.println(e);
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
}
