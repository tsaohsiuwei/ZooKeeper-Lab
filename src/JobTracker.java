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


public class JobTracker {


    static BufferedReader br;
    static BufferedWriter bw;
    static boolean firstProduce;
    static String myPath = "/jobtracker";
    static String root = "/root"; 
    static String workerRoot = "/workerRoot"; 
    static ZkConnector zkc;
    static ZooKeeper zk;
    Watcher watcher;
   
    static InetAddress ip;
    static String IP;


    public static synchronized String consume(String hashcode) throws KeeperException, InterruptedException{

             List<String> list = zk.getChildren(workerRoot,false);
             if(list.size() == 0){
                return "Failed: Failed to complete job";
             }

            Stat stat = zk.exists(root + "/" + hashcode,false);
         
            if(stat != null){

                stat = zk.exists(root + "/" + hashcode + "/answer", false);
                if( stat != null){

                    byte[] b = zk.getData(root + "/" + hashcode + "/answer", false, null);
                    
                    try {
                        String answer = new String(b, "UTF-8");
                        System.out.println("The answer is " + answer);
                        // 1. In progress 2. Password found. 3. Failed 
                        return answer;
                    } catch (UnsupportedEncodingException e){} 
                    
                    
                }
                String answer = "In Progress";
                return answer;
            }

            String answer = "Failed: Job not found";
            return answer;
    }


    public static boolean produce(String hashcode) throws KeeperException, InterruptedException {


            System.out.println("Producing job " + hashcode);

            byte[] value;
            value = hashcode.getBytes();
            
            try{
                zk.create(root + "/" + hashcode, value, Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }catch(KeeperException e){}
           

            return true;
     }



    public static void main(String[] args) {
        try {
            ip = InetAddress.getLocalHost();
            IP = ip.toString();
        } catch (UnknownHostException e) {}

        if (args.length != 1) {
            System.out.println("Wrong usage");
            return;
        }

        JobTracker j = new JobTracker(args[0]);   
        System.out.println("The IP is " + IP);
        j.checkpath();
        try {
			System.out.println("before ss 8888");
            ServerSocket ss = new ServerSocket(8888);
			System.out.println("after 8888 before accept");
        	while(true)
            {

                Socket socket = ss.accept();
                System.out.println("After accepting ClientDriver");

        		br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        		bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                System.out.println("after br, bw");
                String answerForClient;
                System.out.println("before readLine");
				String longmsg = br.readLine();
                System.out.println("The longmsg is: " + longmsg);
                String[] msg = longmsg.split("\\s");
                System.out.println("The msg[0] is " + msg[0] + " and the msg[1] is " + msg[1]);

				if (msg[0].equals("job"))
				{
                    produce(msg[1]);
                    answerForClient = "Job has been submitted successfully";
                    bw.write(answerForClient + "\n");
                    bw.flush();

				}
				else if (msg[0].equals("status"))
				{
                   System.out.println("check status now");
                   answerForClient =  consume(msg[1]); 
                   bw.write(answerForClient + "\n");  
                   bw.flush();
				}
            }
        } catch (Exception e) {} 
    }

    public JobTracker(String host) {

        firstProduce = true;
        zkc = new ZkConnector();
        try {
             zkc.connect(host);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        watcher = new Watcher() { 
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };                 

        try { 
            zk = zkc.getZooKeeper();
            Stat s = zk.exists(root, false);
            if(s == null){
                zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
        }
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
                try{ Thread.sleep(5000); } catch (InterruptedException e) {}
                checkpath(); // re-enable the watch
            }
        }

    }


    private void checkpath() {
        try {
            Stat stat = zk.exists(myPath, watcher);
            if (stat == null) {              // znode doesn't exist; let's try creating it
                System.out.println("Creating " + myPath);
                Code ret = zkc.create(
                            myPath,         // Path of znode
                            IP,           // Data is the IP address
                            CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                            );
                if (ret == Code.OK) System.out.println("the JobTracker -- Primary");
                else System.out.println("Failed to be the JobTracker -- Backup");
            } 
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
        }
    }

}