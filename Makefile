build:
	javac -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar ZkConnector.java ClientDriver.java JobTracker.java FileServer.java Worker.java JobTrackerHandlerThread.java FileServerHandlerThread.java JobPacket.java DictionaryPacket.java
runClientDriver:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. ClientDriver
runJobTracker:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker
runFileServer:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer
runWorker:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker
clean:
	rm -f *.class
