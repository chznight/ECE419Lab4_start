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
	Socket socket=null; //temporarily setting this to null
		
	ArrayList<String> job_list = new ArrayList<String>();
	boolean calculating = true;
	try{
		ObjectInputStream fromServer = new ObjectInputStream(socket.getInputStream());
		JobPacket packetFromServer;
		ObjectOutputStream toServer = new ObjectOutputStream(socket.getOutputStream());

		while(calculating){
			JobPacket packetToServer = new JobPacket();
			System.out.println("Enter 's' to submit Job request, or"); 
			System.out.println("Enter 'q' to submit Job status query:");
			System.out.print("> ");
			BufferedReader buffer=new BufferedReader(new InputStreamReader(System.in));
			String line=buffer.readLine();
			if(line.equals("s")){
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

			}
			if(line.equals("q")){
				for(int i=0;i<job_list.size();i++){
					System.out.println("Querying status of job #"+(i+1)+"...");
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
			}else{
				System.out.println("Invalid input");
				continue;
			}
			
		}		
	}catch(Exception e){
		System.err.println(e);
	}
		
    }

}
