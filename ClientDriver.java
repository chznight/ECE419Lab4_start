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

public class ClientDriver {
    
    static String jobTrackerBoss = "/jobTrackerBoss";
    static String fileServerBoss = "/fileServerBoss";

 //        public static void main(String[] args) {
		
 //        if (args.length != 1) {
 //            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. ClientDriver zkServer:clientPort");
 //            return;
 //        }

 //        ZkConnector zkc = new ZkConnector();
 //        try {
 //            zkc.connect(args[0]);
 //        } catch(Exception e) {
 //            System.out.println("Zookeeper connect "+ e.getMessage());
 //        }
		
 //        ZooKeeper zk = zkc.getZooKeeper(); /*How to find the job tracker ip and port*/
	// 	Socket socket=null; //temporarily setting this to null
		
	// 	ArrayList<String> job_list = new ArrayList<String>();
	// 	boolean calculating = true;
	// 	try{
	// 		Stat s = zk.exists(fileServerBoss, false);
	// 		if (s == null) {
	// 			System.out.println ("fileServerBoss not registered");
	// 			System.exit(1);
	// 		}

	// 		byte[] data = zk.getData (fileServerBoss, false, null);
	// 		String dataString = new String (data);
	// 		String[] ip_port = dataString.split(":");
	// 		int port = Integer.parseInt(ip_port[1]);

	// 		System.out.println (ip_port[0] + " " + port);

	// 		socket = new Socket (ip_port[0], port);
			

	// 		ObjectOutputStream toServer = new ObjectOutputStream(socket.getOutputStream());
	// 		ObjectInputStream fromServer = new ObjectInputStream(socket.getInputStream());
	// 		DictionaryPacket packetFromServer = new DictionaryPacket();
	// 		DictionaryPacket packetToServer = new DictionaryPacket();

	// 		System.out.println ("Connected to file server");
			
	// 		packetToServer.type = DictionaryPacket.DICT_REQUEST;
	// 		packetToServer.index = 265;
	// 		toServer.writeObject(packetToServer);

	// 		packetFromServer = (DictionaryPacket) fromServer.readObject();
	// 		if (packetFromServer.type != DictionaryPacket.DICT_REPLY) {
	// 			System.out.println ("unrecognized packet");
	// 		}

	// 		System.out.println (packetFromServer.content.size());
	// 		System.out.println (packetFromServer.content);

	// }catch(Exception e){
	// 	System.out.println ("Exception");
	// 	System.err.println(e);
	// }
	// }

    public static void main(String[] args) {
		
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. ClientDriver zkServer:clientPort");
            return;
        }

        ZkConnector zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
		
        ZooKeeper zk = zkc.getZooKeeper(); /*How to find the job tracker ip and port*/
		Socket socket2=null; //temporarily setting this to null
		
		ArrayList<String> job_list = new ArrayList<String>();
		boolean calculating = true;
		try{
			Stat s = zk.exists(jobTrackerBoss, false);
			if (s == null) {
				System.out.println ("Job Tracker not registered");
				System.exit(1);
			}

			byte[] data = zk.getData (jobTrackerBoss, false, null);
			String dataString = new String (data);
			String[] ip_port = dataString.split(":");
			int port = Integer.parseInt(ip_port[1]);

			System.out.println (ip_port[0] + " " + port);

			socket2 = new Socket (ip_port[0], port);
			

			ObjectOutputStream toServer = new ObjectOutputStream(socket2.getOutputStream());
			ObjectInputStream fromServer = new ObjectInputStream(socket2.getInputStream());
			JobPacket packetFromServer;


			System.out.println ("Connected to job tracker");
		while(calculating){
			JobPacket packetToServer = new JobPacket();
			System.out.println("Enter 's' to submit Job request, or"); 
			System.out.println("Enter 'q' to submit Job status query:");
			System.out.print("> ");
			BufferedReader buffer=new BufferedReader(new InputStreamReader(System.in));
			String line=buffer.readLine();
			if(line.equals("s")){
				System.out.print("Enter Hash: ");
				line=buffer.readLine();
				job_list.add(line);
				System.out.println("Gonna crack "+line+"! This is job #"+job_list.size());
				packetToServer.type = JobPacket.JOB_SUBMISSION;				
				packetToServer.content=line;
				toServer.writeObject(packetToServer);

				packetFromServer = (JobPacket) fromServer.readObject();

				if(packetFromServer.type==JobPacket.JOB_RECEIVED){
					System.out.println("Submission received");
				}else{
					System.err.println("ERROR: Unknown JOB_* packet!!");
					System.exit(-1);
				}

			} else if(line.equals("q")){
				for(int i=0;i<job_list.size();i++){
					System.out.println("Querying status of job #"+(i+1)+"...");
					packetToServer = new JobPacket();
					packetToServer.type = JobPacket.JOB_QUERY;				
					packetToServer.content=job_list.get(i);
					toServer.writeObject(packetToServer);	

					packetFromServer = (JobPacket) fromServer.readObject();

					if(packetFromServer.type==JobPacket.JOB_CALCULATING){
						System.out.println("Calculating");
					}else if(packetFromServer.type==JobPacket.JOB_FOUND){
						System.out.println("Password found: "+packetFromServer.content);
					}else if(packetFromServer.type==JobPacket.JOB_NOTFOUND){
						System.out.println("Password not found");	
					}else{
						System.err.println("ERROR: Unknown JOB_* packet!!");
						System.exit(-1);
					}

				}
			} else if (line.equals("quit")) {
				calculating = false;
			} else{
				System.out.println("Invalid input");
				continue;
			}
			
		}		
	}catch(Exception e){
		System.out.println ("Exception");
		System.err.println(e);
	}
	}
}
