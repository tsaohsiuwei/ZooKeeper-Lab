import java.io.*;
import java.net.*;
import java.util.*;
import java.lang.*;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.List;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;


public class FileServer extends ZkConnector{


    static String myPath = "/fileserver";
    static ZkConnector zkc;
    static ZooKeeper zk;
    static Watcher watcher;
    static BufferedWriter bw;
    static InetAddress ip;
    static String IP;
    
    public static void main(String[] args) {
		try{     	
			ip = InetAddress.getLocalHost();
            IP = ip.toString();
		
        	if (args.length != 2) {
        	    System.out.println("Wrong usage");
            	return;
        	}

        	FileServer fs = new FileServer(args[0]);
        	fs.checkpath();
             
		
           
			ServerSocket ss = new ServerSocket(7777);
           
			
            
		    
			while (true) {
               	FileInputStream file = new FileInputStream("dictionary/lowercase.rand");
                Scanner p = new Scanner(file);
                System.out.println("Before accepting worker");
				Socket socket = ss.accept();
                System.out.println("After accepting worker");
				bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
				        
				while (p.hasNextLine())
				{
				    String eachLine = p.nextLine();
                   // System.out.println("reading from dictionary: " + eachLine);
				    bw.write(eachLine + "\n");
                    bw.flush();
				}       
				System.out.println("Done sending file and closing socket");
                file.close();
			    socket.close();
			}
        } catch (IOException e) {}
	}
    

    public FileServer(String host) {
        zkc = new ZkConnector();
        try {
             zkc.connect(host);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
        zk = zkc.getZooKeeper();

        watcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                            } };

    }



    private void handleEvent(WatchedEvent event) {
		
			String path = event.getPath();
        	EventType type = event.getType();
        	if(path.equalsIgnoreCase(myPath)) {
            	if (type == EventType.NodeDeleted) {
            	    System.out.println(myPath + " deleted! Let's go!");       
           		    checkpath(); // try to become the boss
            	}
            	if (type == EventType.NodeCreated) {
                	System.out.println(myPath + " created!");       
               		try{ Thread.sleep(5000); } catch (Exception e) {}
                	checkpath(); // re-enable the watch
            	}
        	}
		
        
    }



    private void checkpath() {
		try{
		    Stat stat = zk.exists(myPath, watcher);
		    if (stat == null) {              // znode doesn't exist; let's try creating it
		        System.out.println("Creating " + myPath);
		        Code ret = zkc.create(
		                    myPath,         // Path of znode
		                    IP,           // Data not needed.
		                    CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
		                    );
		        if (ret == Code.OK) System.out.println("the FileServer!");
		    } 
    	} catch (KeeperException e) {
		} catch (InterruptedException e) {
		}
}
}
