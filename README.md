# RMI Multi-Server Program for String Operations with Paxos Consensus and Fault Tolerance

## Overview
This Java program implements a distributed key-value store using Java RMI (Remote Method Invocation) with the Paxos Consensus Algorithm. The system consists of multiple Server Replicas, each running its own Proposer, Acceptor, and Learner roles to achieve consensus before performing key-value store operations (PUT, GET, DELETE) in a consistent and fault-tolerant manner.

1) **Prepare Phase**: The proposer sends a prepare message to all acceptors to get a majority promise.
2) **Accept Phase**: If the majority promised, the proposer sends an accept request with the operation value.
3) **Learn Phase**: Upon receiving majority accepts, the proposer informs all learners to commit the operation.

On the server side, Java RMI handles multithreading, allowing multiple client requests to be processed concurrently. On the client side, we use thread pools to send concurrent requests to the server, improving performance. A `ConcurrentHashMap` is used on the server to ensure thread-safe access to the key-value store.

We use one thread pool for prepopulation of data, one for the batch of 5 PUTs, 5 GETs, and 5 DELETEs, and one for taking in user commands and executing them concurrently.

## Files
- **RMIServer.java**: Initializes the RMI registry, creates and registers 5 Server Replicas.
- **ClientOne.java**: Connects to one of the replicas, sends commands, and displays the server results. Uses a fixed-thread pool to send concurrent requests and simulate realistic concurrent access.
- **ClientTwo.java**: Second client for testing replication consistency across replicas.
- **KeyValueStoreOperations.java**: Handles the actual PUT, GET, DELETE logic. Implements the KeyValueStoreOperation interface.
- **ServerReplica.java**: Implements the ServerReplicaInterface interface. Each replica runs its own Paxos roles and interacts with other replicas to achieve quorum.
- **Acceptor.java, Proposer.java, Learner.java** – Implements Paxos roles. Each server has one instance of each. Failures are injected probabilistically, i.e. 50% for Acceptor, 40% for others, to simulate real-world fault tolerance.

## Paxos Implementation with Failure Simulation
- Five ServerReplicas run on separate ports, each with its own Proposer, Acceptor, and Learner. 
- The Proposer starts the consensus by sending a prepare request to Acceptors with a unique proposal number. 
- Acceptors respond with a promise if they haven't already promised a higher proposal, optionally sharing their last accepted value. 
- If a majority responds, the Proposer sends an accept request; if accepted by a majority, Learners apply the update. 
- Failures are randomized:
  - Acceptors fail with a 50% chance during prepare or accept. 
  - Proposers and Learners fail with a 40% chance. 
  - Failures last 3–5 seconds, after which roles recover automatically.
- The system remains consistent and fault-tolerant despite transient failures and no central coordinator.

## How to Run
Unzip the files and store in your desired location. Then proceed with the given steps:

### 1. **Run the RMI Server**:
1. Open Terminal 1 to run the RMI server.
2. Navigate to the folder where the code is saved.
3. Compile the server:
```bash
javac RMIServer.java
```
4. Provide the base port number:
```bash 
java RMIServer <port>
```
Once started, 5 Server Replicas will be created starting at `<port>` up to `<port + 4>`.

### 2. **Run the First Client**:
1. Open Terminal 2.
2. Compile the client:
```bash
javac ClientOne.java
```
3. Connect to a server:
```bash
java ClientOne <server address> <port>
```

### 3. **Key-Value Store Operations**:
The client first prepopulates the key-value store with:

```
put 1 panda
put 2 bird
put 3 cat
put 4 dog
put 5 fish
```

Then performs:

```
put 6 monkey
put 7 elephant
put 8 raccoon
put 9 swan
put 10 duck
get 6 to get 10
delete 6 to delete 10
```

Each operation triggers a Paxos proposal to reach consensus across all replicas. Simulated failures may cause consensus failures for some operations.

Once done, the client prompts for user input:

```
Enter the operation followed by arguments: (Put key value, Get key, Delete key) or type 'close' to exit client:
```

## Commands and Outputs

### Terminal 1 (Server):
```bash
javac RMIServer.java
java RMIServer 3000
```
You will now see this message:
```
[2025-04-12 21:22:40.135] [main] All 5 server replicas started and configured with Paxos.
[2025-04-12 21:22:40.135] [main] Available Paxos server replicas:
[2025-04-12 21:22:40.135] [main] ServerReplica-1 running on port: 3000
[2025-04-12 21:22:40.1351 [main] ServerReplica-2 running on port: 3001
[2025-04-12 21:22:40.135] [main] ServerReplica-3 running on port: 3002
[2025-04-12 21:22:40.135] [main] ServerReplica-4 running on port: 3003
[2025-04-12 21:22:40.135] [main] ServerReplica-5 running on port: 3004
```

### Terminal 2 (Client):
```bash
javac ClientOne.java
java ClientOne localhost 3001
```

You will now see this message:
```
[2025-04-12 21:25:01.236] [main] ClientOne: Connected to RMI server: ServerReplica-2 on port: 3001
```

Now you will see all the responses for the prepopulation of data task as well as 5 PUTs, 5 GETs and 5 DELETEs. Some may fail due to Paxos consensus failure (if quorum isn’t reached because of simulated failures.

Next you will be prompted to enter your command as follows:
```
Enter the operation followed by arguments: (Put key value, Get key, Delete key) or type 'close' to exit client:
```

Request:
```bash
put 15 apple
```
### Response on Server
Logs from all replicas showing Paxos proposal progress — prepare, accept, learn — with timestamps.
If consensus is acheived you will see this:
```
[2025-04-12 21:27:20.369] [RMI TCP Connection(4)-127.0.0.1] ServerReplica-2 [Proposer]: Final consensus for proposal #750919017: PUT:15:a
[2025-04-12 21:27:20.369] [RMI TCP Connection(4)-127.0.0.1] Successfully added key: 15 with value: a
```
If failures occur you will see this:
```
[2025-04-12 22:15:34.310] [Thread-0] ServerReplica-1 [Acceptor]: Acceptor recovered from failure
[2025-04-12 22:15:34.717] [Thread-2] ServerReplica-2 [Acceptor]: Simulating failure during prepare phase
[2025-04-12 22:15:37.073] [Thread-1] ServerReplica-1 [Learner]: Learner recovered from failure
[2025-04-12 22:15:37.934] [Thread-8] ServerReplica-5 [Acceptor]: Simulating failure during prepare phase
[2025-04-12 22:15:38.116] [Thread-14] ServerReplica-5 [Proposer]: Proposer recovered from failure
[2025-04-12 22:15:38.554] [Thread-7] ServerReplica-4 [Learner]: Simulating learner failure
[2025-04-12 22:15:40.592] [Thread-4] ServerReplica-3 [Acceptor]: Acceptor recovered from failure
[2025-04-12 22:15:40.612] [Thread-6] ServerReplica-4 [Acceptor]: Simulating failure during accept phase
[2025-04-12 22:15:40.815] [Thread-11] ServerReplica-2 [Proposer]: Proposer recovered from failure
```

### Response on Client
```
[2025-04-12 21:27:20.372] [pool-3-thread-1] ClientOne: Response from server: ClientOne ServerReplica-2 successfully PUT key=15 with value=a
```
If the operation fails due to consensus issues you will see this:
```
[2025-04-12 22:12:09.195] [pool-2-thread-2] ClientOne: Response from server: ClientOne ServerReplica-2 PUT operation failed - consensus failure
```

### Terminal 3 (Second Client):
Connect to another server (e.g., port 3002) and test consistency:
```bash
javac ClientTwo.java
java ClientTwo localhost 3002
get 15
```
You are now connected to a different server from a different client and can test consistency to see if commands executed on other servers have propagated to this server.
If consensus was achieved, key 15 will be available on this replica too.

(For detailed complete logs with prepare and commit phase logs from coordinator, see next section. This section showed onnly the final log for every action.)

## Example

### 1. Server startup and registration of replicas:
All 5 replicas start on consecutive ports.
![img.png](images/img.png)

### 2. Client prepopulation and 15 PUTs, GETs and DELETEs:
Prepopulates the store and performs 15 operations with simulated Paxos failures.
![img2.png](images%2Fimg2.png)

### 3. Server side logs for prepopulation and 15 PUTs, GETs and DELETEs:
![img7.png](images%2Fimg7.png)

### 3. Client manual operation:
Handles additional manual operations interactively.
![img3.png](images%2Fimg3.png)

### 4. Server logs for manual operation:
Show Paxos phases — prepare, accept, and learn — along with consensus or failure decisions.
![img4.png](images%2Fimg4.png)
![img5.png](images%2Fimg5.png)

### 5. Consistency testing:
After a PUT from ClientOne, a GET from ClientTwo on a different replica shows the value if consensus was achieved.
![img6.png](images%2Fimg6.png)

### 6. Simulated Failure logs when no operation is in progress
![img1.png](images%2Fimg1.png)

---
