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
public class JobTrackerHandlerThread extends Thread {

	private Socket socket = null;
	private String zkconnect;

    static String squenceNumDispenser = "/squenceNumDispenser";
    static String myTasks = "/tasks";
    static String workerIdDispenser = "/workerIdDispenser";
    static String jobTrackerBoss = "/jobTrackerBoss";
    static String availableWorkers = "/availableWorkers";
    static String inProgress = "/inProgress";
    static String finishedJobs = "/finishedJobs";  
	static String tempResults = "/tempResults";  

	public JobTrackerHandlerThread(Socket socket, String zkconnect_) {
		super("JobTrackerHandlerThread");
		this.socket = socket;
		this.zkconnect = zkconnect_;
		System.out.println("Created new Thread to handle client");
	}

	public void run() {

		boolean gotByePacket = false;
		ZkConnector zkc = new ZkConnector();
        try {
            zkc.connect(zkconnect);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        ZooKeeper zk = zkc.getZooKeeper();

		try {
			ObjectInputStream fromClient = new ObjectInputStream(socket.getInputStream());
			ObjectOutputStream toClient = new ObjectOutputStream(socket.getOutputStream());
			JobPacket packetFromClient;

			while (( packetFromClient = (JobPacket) fromClient.readObject()) != null) {
				JobPacket packetToClient = new JobPacket();
				
				if(packetFromClient.type == JobPacket.JOB_SUBMISSION) {
					System.out.println("(SUBMISSION) From Client: " + packetFromClient.content);				

		            //Here we start to look at the list of availible works
		            //If a worker is aviliable, we place a task in his work folder: /tasks/work#/
		            //everyone who is avilible has a task placed in his folder
		            List<String> list = zk.getChildren(availableWorkers, true);
		            while (list.size() == 0) {
		                Thread.sleep (5000);
		                list = zk.getChildren(availableWorkers, true);
		            }

		            int num_nodes = list.size();
		            int num_chunks_per_node = 266/num_nodes;
		            String workerBee;
		            String allWorkersOfCurrentTask = "";
		            String squenceNumOfCurrentTask = "";
		            String taskMetaData;
		            for (int i = 0; i < num_nodes; i++) {
		                workerBee = list.get(i);
		                Stat stat = zk.setData (squenceNumDispenser, "nothing".getBytes(), -1);
		                //create a task with a unique task number, we will remember this number later for checking if tasks are finished
		                allWorkersOfCurrentTask = allWorkersOfCurrentTask + workerBee + " ";
		                squenceNumOfCurrentTask = squenceNumOfCurrentTask + stat.getVersion() + " ";

		                String chunck_start_end; //store where to begin, where to end, and hash
		                if (i == (num_nodes - 1)) {
		                	chunck_start_end = i*num_chunks_per_node + " " + 266 + " " + packetFromClient.content;
		                } else {
		                	chunck_start_end = i*num_chunks_per_node + " " + ((i+1)*num_chunks_per_node) + " " + packetFromClient.content;
		                }
		                
		                zk.create(
		                    myTasks + "/" + workerBee + "/task" + stat.getVersion(),
		                    chunck_start_end.getBytes(),         
		                    Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
		                    CreateMode.PERSISTENT   // Znode type, set to Persistent.
		                    );
		            }

		            //This stores the state of the task in the inProgress dicrectory
		            taskMetaData = allWorkersOfCurrentTask + ":" + squenceNumOfCurrentTask;
		            Stat s = zk.exists (inProgress + "/" + packetFromClient.content, false);
		            if (s == null) {
		                zk.create(
		                    inProgress + "/" + packetFromClient.content,
		                    taskMetaData.getBytes(),         
		                    Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
		                    CreateMode.PERSISTENT   // Znode type, set to Persistent.
		                    );
					}
					/* process the job here */

					//packetToClient.type = JobPacket.JOB_RECEIVED;
					packetToClient.type = JobPacket.JOB_RECEIVED; //just send back this

					toClient.writeObject(packetToClient);
					continue;
				}

				if(packetFromClient.type == JobPacket.JOB_QUERY) {
					System.out.println("(QUERY) From Client: " + packetFromClient.content);

					//check if its in the finished directory
					Stat s = zk.exists(finishedJobs + "/" + packetFromClient.content, false);
					if (s != null) {
						byte[] data = zk.getData (finishedJobs + "/" + packetFromClient.content, false, null);
						String dataString = new String (data);
						if (dataString.equals("NOT_FOUND")) {
							packetToClient.type = JobPacket.JOB_NOTFOUND;
							toClient.writeObject(packetToClient);
							continue;
						} else {
							packetToClient.type = JobPacket.JOB_FOUND;
							packetToClient.content = dataString;
							toClient.writeObject(packetToClient);
							continue;
						}
					}

					s = zk.exists(inProgress + "/" + packetFromClient.content, false);
					if (s != null) {
						byte[] data = zk.getData (inProgress + "/" + packetFromClient.content, false, null);
						String taskMetaData = new String (data);
						System.out.println ("task Meta Data: " + taskMetaData);
						String [] tempTokens = taskMetaData.split(":");
						String [] workersIDs = tempTokens[0].split("[ ]+");
						String [] squenceNumsOfTask = tempTokens[1].split("[ ]+");

						//check if done, if done, delete it from inProgess and put into finishedJobs
			            List<String> list = zk.getChildren(tempResults, true);
			            if (list.size() == 0) {
			            	packetToClient.type = JobPacket.JOB_CALCULATING;
							toClient.writeObject(packetToClient);
							continue;			            
						}

						int totalTasksCounted = 0;
						boolean found = false;
						packetToClient.type = JobPacket.JOB_CALCULATING;
						for (int i = 0; i < list.size(); i++) {
							String tempComplTask = list.get(i);
							for (int j = 0; j < squenceNumsOfTask.length; j++) {
								if (tempComplTask.equals("task"+squenceNumsOfTask[j])) {
									data = zk.getData (tempResults + "/task" + squenceNumsOfTask[j], false, null);
									String dataString = new String (data);
									if (dataString.equals("NOT_FOUND")) {
										totalTasksCounted++;
									} else {
										found = true;
										packetToClient.type = JobPacket.JOB_FOUND;
										packetToClient.content = dataString;
									}
								}
							}
						}
						if (totalTasksCounted == squenceNumsOfTask.length) {
							if (found == false) {
								packetToClient.type = JobPacket.JOB_NOTFOUND;
							}
						}
						//check if nodes are down, if they are reasign tasks
						//reply accordingly
					} else {
						System.out.println ("job lost?!");
					}

					// query status of job here


					//packetToClient.type = JobPacket.JOB_CALCULATING;
					//packetToClient.type = JobPacket.JOB_FOUND;
					//packetToClient.content = password;
					//packetToClient.type = JobPacket.JOB_NOTFOUND;
					

					toClient.writeObject(packetToClient);
					continue;
				}
				
				System.err.println("ERROR: Unknown JOB_* packet!!");
				System.exit(-1);
			}
			
			fromClient.close();
			toClient.close();
			socket.close();

		} catch (IOException e) {
			if(!gotByePacket)
				e.printStackTrace();
		} catch (ClassNotFoundException e) {
			if(!gotByePacket)
				e.printStackTrace();
		} catch(KeeperException e) {
            System.out.println(e.code());
        } catch (InterruptedException e) {

        }
	}
}
