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


public class Worker{


    // before start computing, create znode under password hash. e.g. password/answer
    // set the data to "In Progress" 
    // after computing, use setData to set the final answer

    static String root = "/root"; 
    static String workerRoot = "/workerRoot"; 
    static ZkConnector zkc;
    static ZooKeeper zk;
    static Watcher watcher;
    static BufferedReader br;
    static Socket sock;
    static String hostname; 
    static byte[] hostnameinbyte;
    static boolean waitConsume;
    static boolean waitRoot;
    static boolean waitFileserver;




    public Worker(String host) {
        zkc = new ZkConnector();
        try {
             zkc.connect(host);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        watcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };

        zk = zkc.getZooKeeper();
        waitRoot = true;
        waitConsume = true;
        waitFileserver = true;
        try {
            Stat stat = zk.exists(workerRoot, false);
            if (stat == null){
                zk.create(workerRoot, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            zk.create(workerRoot + "/worker", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
        }
    }


    public static synchronized String consume() throws KeeperException, InterruptedException{
                
                String retvalue;
                Stat stat = null;
                String INPROGRESS = "In Progress";
                boolean acquiredTask;
                 
                // Get the first element available
                while (true) {
                    
                        List<String> list = zk.getChildren(root, false);
                        if (list.size() == 0) {
                            waitConsume = true;
                            zk.getChildren(root, watcher);
                            System.out.println("Going to wait");
                            while(waitConsume){
                                try{
                                    Thread.sleep(3000);
                                } catch(InterruptedException e){}
                            }
                        } 

                            List<String> answerNode;
                            int i;
                            acquiredTask = false;

                            for(i = 0; i < list.size(); i++){
                               
                                answerNode = zk.getChildren(root + "/" +  list.get(i), false);
                                if(answerNode.size() == 0){ // this job is available for worker
                                    acquiredTask = true;
                                    break;
                                }
                            }

                                
                                // If the job one picks is being processed, it will go to the else if statement
                                Code ret = null;
                                if(acquiredTask == true){   
                        
                                 ret = zkc.create(root +"/" + list.get(i) + "/answer", INPROGRESS, CreateMode.EPHEMERAL);
                                 System.out.println("the ret is " + ret);
                                
                                 System.out.println("after creating answernode");
                                }
                                else 
                                    continue;
                                

                                if(ret == Code.OK) 
                                {
                                    System.out.println("ret code is OK, and going to process this job");
                                    byte[] b = zk.getData(root + "/" + list.get(i),false, null);
                                    try{
                                        retvalue = new String(b, "UTF-8");
                                        return retvalue;
                                    }catch(UnsupportedEncodingException e){}
                                    
                                }

                                else {
                                    System.out.println("ret code is not OK; continue");
                                    continue;
                                } 

                         

                            // deal with fault tolerance above. 
                            // since the hashcode/answer is ephemeral type, when worker dies, its answer node also dies. 
                            // therefore the job becomes available again; other workers can process it.
                            
                        
                    
                }

    }


    public static void main(String[] args) {
          
            if (args.length != 1) {
                System.out.println("Wrong usage");
                return;
            }

            Worker w = new Worker(args[0]);   
            
            String job; // this is the password hash submitted by client.
            job = w.checkpath();
            System.out.println("Going to process for the first time: " + job);

            /// processing job. interact with fileserver
            Stat stat = null;
            try {
                stat = zk.exists("/fileserver", watcher);
            } catch (KeeperException e) {
            } catch (InterruptedException e) {
            }

            if(stat == null)
            {
                while(waitFileserver)
                {
                    try {
                        Thread.sleep(3000);
                        }catch(InterruptedException e){}
                }
            }
            HashMap<String, String> dict = new HashMap<String, String>();
            try {
                System.out.println("going to get IP from /fileserver");
                hostnameinbyte = zk.getData("/fileserver",false, null);
                hostname = new String(hostnameinbyte);
                String [] host = hostname.split("\\.");
                System.out.println("Before connecting to the fileserver");
                sock = new Socket(host[0], 7777);
                System.out.println("After connecting to the fileserver");
                br =  new BufferedReader(new InputStreamReader(sock.getInputStream()));
                
                String word = br.readLine();
                while (word != null){
                 //   System.out.println("Reading from fileserver: " + word);
                    String answer;
                    answer = MD5Test.getHash(word);
                    //System.out.println("Getting Hash: " + answer);
                    dict.put(answer,word);
                    word = br.readLine();
               
                }
            } catch (KeeperException e) {
            } catch (UnknownHostException e) {
            } catch (IOException e) {
            } catch (InterruptedException e) {
            }
       
            while (true) {
                try{ 
                    boolean found = false;
                    System.out.println("Going to compare with the dictionary"); 
                    if (dict.containsKey(job)) 
                    {
                        System.out.println("Found the answer");
                        found = true;
                    }
                    
                    byte[] b = null;

                    if (found == false){

                        String s = "Failed: Password not found";
                        b = s.getBytes();
                        
                    }
                    if(found == true){
                        String s = "Password found: " + dict.get(job);;
                        b = s.getBytes();
                    }

                    System.out.println("Done. goint to set data");
                   // System.out.println("The Answer is " + b);
                    zk.setData(root + "/" + job + "/answer", b, zk.exists(root + "/" + job + "/answer", false).getVersion());

                    System.out.println("Done setting data, begining the next task");
                    job = consume();
                     
                 


                }catch (Exception e) {}
            }

            //sock.close();      
    }



    private void handleEvent(WatchedEvent event) {
       

        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(root)) {
   
            if (type == EventType.NodeCreated) {
                System.out.println(root + " created!");       
                waitRoot = false;
                
            }

            if (type == EventType.NodeChildrenChanged){

                System.out.println("The root child is created!");         
                waitConsume = false;

            }

        }
        if(path.equals("/fileserver")){
            if (type == EventType.NodeCreated) {
                System.out.println("File Server is created");
                waitFileserver = false;
            }
            if (type == EventType.NodeDeleted){
                System.out.println("File Server is deleted");
                try{
                    Thread.sleep(3000);
                    hostnameinbyte = zk.getData("/fileserver",false, null);
                    hostname = new String(hostnameinbyte);
                    String [] host = hostname.split("\\.");
                    sock = new Socket(host[0], 7777);
                    br =  new BufferedReader(new InputStreamReader(sock.getInputStream()));
                } catch (KeeperException e) {
                } catch (UnknownHostException e) {
                } catch (IOException e) {
                } catch (InterruptedException e){
                }
            } 

        }
    }



    private String checkpath() {
        Stat stat = zkc.exists(root, watcher);
        if (stat == null) {   
            while(waitRoot){
                try{
                    Thread.sleep(3000);
                }catch(InterruptedException e){}
            }
        } 
        
        
        String job = null; 
        try {
            job = consume();
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
        }
         
        return job;   


    }




}