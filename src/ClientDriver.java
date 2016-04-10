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


public class ClientDriver {


     static String myPath = "/jobtracker";
     static ZkConnector zkc;
     static ZooKeeper zk;
     static BufferedReader br = null;
     static BufferedWriter bw = null;
     static Socket sock = null;
     static boolean NojobTracker;

     public static void main(String[] args) {

 	

            if (args.length != 3) {
                System.out.println("Usage: java ClientDriver <IP Address of ZK>:<Port> job/status password");
                return;
            }
            zkc = new ZkConnector();
            try {
                zkc.connect(args[0]);
            } catch(Exception e) {
                System.out.println("Zookeeper connect "+ e.getMessage());
            }

            zk = zkc.getZooKeeper();
            try {
                zk.exists(
                    myPath, 
                    new Watcher() {       // Anonymous Watcher
                        @Override
                        public void process(WatchedEvent event) {
                            // check for event type NodeCreated
                            boolean isNodeCreated = event.getType().equals(EventType.NodeCreated);
                            boolean isNodeDeleted = event.getType().equals(EventType.NodeDeleted);
                           
                            boolean isMyPath = event.getPath().equals(myPath);
                            if (isNodeCreated && isMyPath) {
                                System.out.println(myPath + " created!");
                                NojobTracker = false;
                            }

                            if(isNodeDeleted && isMyPath){
                            
                                System.out.println(myPath + " deleted");
                                try
                                {   
                                    
                                    Thread.sleep(5000);
                                    
                                    byte[] hostnameinbyte = zk.getData(myPath,false, null);
                                    String hostname = new String(hostnameinbyte, "UTF-8");
                                    String [] host = hostname.split("\\.");
                                    sock = new Socket(host[0],8888);
                                    br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                                    bw = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream()));
                                }
                                catch(InterruptedException e){}
                                catch(KeeperException e){}
                                catch(UnsupportedEncodingException e){}
                                catch(UnknownHostException e){}
                                catch(IOException e){}
                            }
                        }
                    });
            } catch(KeeperException e) {
                System.out.println(e.code());
            } catch(Exception e) {
                System.out.println(e.getMessage());
            }

            System.out.println("Waiting for " + myPath + " to be created ...");
            
            try{       
                Stat stat = zk.exists(myPath, true);
                if(stat == null){
                   NojobTracker = true;
                  while(NojobTracker){
                        Thread.sleep(3000);
                  }
                }
            } catch(KeeperException e) {
                System.out.println("Keeper Exception");
            } catch(InterruptedException e){}

            System.out.println("DONE, ready to submit job request"); // After this, we can submit job requests
            String result = null;
            try{
                byte[] hostnameinbyte = zk.getData(myPath,false, null);
                
                String hostname = new String(hostnameinbyte);
                String [] host = hostname.split("\\.");
                System.out.println("The host0 is " + host[0]);
                sock = new Socket(host[0], 8888);
                System.out.println("After connecting to the jobtracker");

                br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                bw = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream()));
                bw.write(args[1]+" "+ args[2] + "\n");
                bw.flush();

                result = br.readLine();
            }
            catch(KeeperException e){}
            catch(UnknownHostException e){}
            catch(IOException e){}
            catch(InterruptedException e){}
            
            System.out.println(result);



            // start interative command line input
            System.out.println("Enter job/status <password hash>. Or 'q' to exit");
            System.out.print("> ");
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

       
            try{

                String userInput = in.readLine();
                while( userInput != null && !userInput.equals("q")){

                    String[] msg = userInput.split("\\s");
                    if(msg[0].equals("job") || msg[0].equals("status")){ 

                        System.out.println("msg0 is " + msg[0]);
                        bw.write(userInput + "\n");
                        bw.flush();

                        result = br.readLine();
                        System.out.println(result);
                    }

                    System.out.println("Enter job/status <password hash>. Or 'q' to exit");
                    System.out.print("> "); 
                     
                    userInput = in.readLine();
                }

                System.out.println("ClientDriver is quitting");
               
                sock.close();
            }
            catch(IOException e){}  
    }
}  