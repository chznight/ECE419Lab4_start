build:
	javac -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar ZkConnector.java JobTracker.java Worker.java
runJobTracker:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker
runWorker:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker
clean:
	rm -f *.class
