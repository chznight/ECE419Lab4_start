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
import java.util.HashMap;

import java.io.IOException;

public class FileServer {
    
    static String availableWorkers = "/availableWorkers";
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer zkServer:clientPort <File name of dictionary>");
            return;
        }

        ZkConnector zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

	try{
		ArrayList<String> dictionary_list = new ArrayList<String>();
		FileReader dictionary_file_reader = new FileReader(args[1]);
		BufferedReader reader = new BufferedReader(dictionary_file_reader);
		String line;
		line = reader.readLine();
		int lol=0;
		while (line != null) {
			dictionary_list.add(line);
	        	line = reader.readLine();
	        }
	        reader.close();

		ArrayList<ArrayList<String>> partition_list = new ArrayList<ArrayList<String>>();
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

        ZooKeeper zk = zkc.getZooKeeper();

       	/*Need to send words to workers*/
    }
}
