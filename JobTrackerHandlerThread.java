import java.net.*;
import java.io.*;

public class JobTrackerHandlerThread extends Thread {

	private Socket socket = null;
	private String zkconnect;

    static String squenceNumDispenser = "/squenceNumDispenser";
    static String myTasks = "/tasks";
    static String workerIdDispenser = "/workerIdDispenser";
    static String jobTrackerBoss = "/jobTrackerBoss";
    static String availableWorkers = "/availableWorkers";

	public JobTrackerHandlerThread(Socket socket, String zkconnect_) {
		super("JobTrackerHandlerThread");
		this.socket = socket;
		this.zkconnect = zkconnect_;
		System.out.println("Created new Thread to handle client");
	}

	public void run() {

		boolean gotByePacket = false;
		
		try {
			ObjectInputStream fromClient = new ObjectInputStream(socket.getInputStream());
			JobPacket packetFromClient;
			
			ObjectOutputStream toClient = new ObjectOutputStream(socket.getOutputStream());
			

			while (( packetFromClient = (JobPacket) fromClient.readObject()) != null) {
				JobPacket packetToClient = new JobPacket();
				
				if(packetFromClient.type == JobPacket.JOB_SUBMISSION) {
					System.out.println("(SUBMISSION) From Client: " + packetFromClient.content);
					
					// process the job here

					packetToClient.type = JobPacket.JOB_RECEIVED;
					toClient.writeObject(packetToClient);
					continue;
				}

				if(packetFromClient.type == JobPacket.JOB_QUERY) {
					System.out.println("(QUERY) From Client: " + packetFromClient.content);

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
		}
	}
}
