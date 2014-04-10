import java.util.*; 
import java.net.*;
import java.io.*;
import java.util.List;

import java.io.IOException;

public class FileServerHandlerThread extends Thread {

	private Socket socket = null;
	private String zkconnect;
	private ArrayList<ArrayList<String>> partition_list;

    static String squenceNumDispenser = "/squenceNumDispenser";
    static String myTasks = "/tasks";
    static String workerIdDispenser = "/workerIdDispenser";
    static String jobTrackerBoss = "/jobTrackerBoss";
    static String availableWorkers = "/availableWorkers";

	public FileServerHandlerThread(Socket socket, String zkconnect_, ArrayList<ArrayList<String>> partition_list_) {
		super("FileServerHandlerThread");
		this.socket = socket;
		this.zkconnect = zkconnect_;
		this.partition_list=partition_list_;
		System.out.println("Created new Thread to handle client");
	}

	public void run() {
		
		try {
			ObjectInputStream fromClient = new ObjectInputStream(socket.getInputStream());
			DictionaryPacket packetFromClient;
			
			ObjectOutputStream toClient = new ObjectOutputStream(socket.getOutputStream());
			
			while (( packetFromClient = (DictionaryPacket) fromClient.readObject()) != null) {
				DictionaryPacket packetToClient = new DictionaryPacket();
				
				if(packetFromClient.type == DictionaryPacket.DICT_REQUEST) {
					System.out.println("(REQUEST) From Client: " + packetFromClient.content);
					
					packetToClient.type = DictionaryPacket.DICT_REPLY;

					packetToClient.content=new ArrayList<String>(partition_list.get(packetFromClient.index));

					toClient.writeObject(packetToClient);
					continue;
				}
				
				System.err.println("ERROR: Unknown DICT_* packet!!");
				System.exit(-1);
			}
			
			fromClient.close();
			toClient.close();
			socket.close();

		} catch (Exception e){
			System.err.println(e);
		}
	}
}
