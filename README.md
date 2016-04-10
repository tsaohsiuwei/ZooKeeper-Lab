# ZooKeeper-Lab
A distributed fault-tolerant data processing framework using the Apache ZooKeeper service

ClientDriver initiates job request/status query to the JobTracker.
Primary JobTracker submits job to the ZooKeeper, and let workers process it.

When a worker starts, it registers itself with ZK under workerRoot. 
Worker fetches dictionary file from the FileServer, and stores each word/hashed word pair in the local HashMap.

When a FileServer starts, it tries to register itself as primary, if this fails, this is probably because a primary exists, and it becomes secondary.
