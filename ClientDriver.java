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

public class ClientDriver {
    
    static String jobTrackerBoss = "/jobTrackerBoss";
    static String fileServerBoss = "/fileServerBoss";
    static ZooKeeper zk;
    static ZkConnector zkc;
    static Watcher watcher;
    static CountDownLatch nodeCreatedSignal;

    static Socket socket = null;
    static ObjectOutputStream toServer;
    static ObjectInputStream fromServer;

    public static void main(String[] args) {
		
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. ClientDriver zkServer:clientPort");
            return;
        }
		nodeCreatedSignal = new CountDownLatch(1);
        zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
		
        zk = zkc.getZooKeeper(); /*How to find the job tracker ip and port*/
		
		ArrayList<String> job_list = new ArrayList<String>();
		boolean calculating = true;
        watcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };		

		System.out.println ("Waiting for Job Tracker server");

        checkpath();

		JobPacket packetFromServer;

		while(calculating){
			try {
				nodeCreatedSignal.await();
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
			} catch (IOException e) {
				System.out.println("Server went down, wait for backup");
			} catch (Exception e) {
				System.out.println("Server went down, wait for backup");
				System.out.println(e.getMessage());
			}
		}		
	}

    public static void checkpath() {
        try {
            Stat stat = zkc.exists("/jobTrackerBoss", watcher);
            if (stat != null) {
				byte[] data = zk.getData (jobTrackerBoss, false, null);
				String dataString = new String (data);
				String[] ip_port = dataString.split(":");
				int server_port = Integer.parseInt(ip_port[1]);
				socket = new Socket (ip_port[0], server_port);
				toServer = new ObjectOutputStream(socket.getOutputStream());
				fromServer = new ObjectInputStream(socket.getInputStream());
			    nodeCreatedSignal.countDown();
			    System.out.println("Connected, some jobs might have to be resent, press Enter continue: ");
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
        if(path.equalsIgnoreCase(jobTrackerBoss)) {
            if (type == EventType.NodeDeleted) {
                nodeCreatedSignal = new CountDownLatch(1);
                System.out.println(jobTrackerBoss + " down");
            }
            if (type == EventType.NodeCreated) {
                System.out.println(jobTrackerBoss + " back up, will reconnect");
            }
            checkpath(); // re-enable the watch
            
        }
    }
}
