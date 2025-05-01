import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import Paxos.Acceptor;
import Paxos.Learner;
import Paxos.Proposer;

/**
 * Represents a single replica in the Paxos-based key-value store.
 * Handles PUT, GET, and DELETE requests using Paxos consensus.
 */
public class ServerReplica extends UnicastRemoteObject implements ServerReplicaInterface {
  private final String replicaId;
  private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
  private final KeyValueStoreOperations keyValueStore;
  private Acceptor localAcceptor;
  private Learner localLearner;
  private Proposer proposer;
  private List<Acceptor> allAcceptors;
  private List<Learner> allLearners;

  /**
   * Constructs a ServerReplica with its own local Paxos components.
   *
   * @param replicaId The ID of this server replica.
   * @throws RemoteException If RMI communication fails.
   */
  public ServerReplica(String replicaId) throws RemoteException {
    super();
    this.replicaId = replicaId;
    this.keyValueStore = new KeyValueStoreOperations();
    this.localAcceptor = new Acceptor(replicaId);
    this.localLearner = new Learner(replicaId);
  }

  /**
   * Configures this replica's proposer with references to all acceptors and learners.
   *
   * @param allAcceptors List of all acceptors across replicas.
   * @param allLearners List of all learners across replicas.
   */
  public void configurePaxos(List<Acceptor> allAcceptors, List<Learner> allLearners) {
    this.allAcceptors = allAcceptors;
    this.allLearners = allLearners;
    this.proposer = new Proposer(replicaId, allAcceptors, allLearners);
    log("Configured proposer with " + allAcceptors.size() + " acceptors and " + allLearners.size() + " learners");
  }

  /**
   * Returns the local Acceptor instance.
   */
  public Acceptor getLocalAcceptor() {
    return localAcceptor;
  }

  /**
   * Returns the local Learner instance.
   */
  public Learner getLocalLearner() {
    return localLearner;
  }


  /**
   * Logs a message with a timestamp and thread name.
   * @param message The message to log.
   */
  private void log(String message) {
    String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
    String threadName = Thread.currentThread().getName();
    System.out.println("[" + timestamp + "] [" + threadName + "] " + replicaId + ": " + message);
  }

  /**
   * Performs a PUT operation after reaching consensus using Paxos.
   *
   * @param key The key to store.
   * @param value The value to associate with the key.
   * @param clientName The client initiating the request.
   * @param participantName The replica name handling the request.
   * @return A success or failure message.
   * @throws RemoteException If an RMI error occurs.
   */
  @Override
  public String put(String key, String value, String clientName, String participantName) throws RemoteException {
    log(clientName + " " + participantName + " received PUT request for key=" + key + ", value=" + value);
    boolean consensus = proposer.propose("PUT", key, value);
    if (consensus) {
      keyValueStore.put(key, value, replicaId, store);
      return clientName + " " + participantName + " successfully PUT key=" + key + " with value=" + value;
    } else {
      return clientName + " " + participantName + " PUT operation failed - consensus failure";
    }
  }

  /**
   * Performs a DELETE operation after reaching consensus using Paxos.
   *
   * @param key The key to delete.
   * @param clientName The client initiating the request.
   * @param participantName The replica name handling the request.
   * @return A success or failure message.
   * @throws RemoteException If an RMI error occurs.
   */
  @Override
  public String delete(String key, String clientName, String participantName) throws RemoteException {
    log(clientName + " " + participantName + " received DELETE request for key=" + key);
    boolean consensus = proposer.propose("DELETE", key, "");
    if (consensus) {
      keyValueStore.delete(key, replicaId, store);
      return clientName + " " + participantName + " successfully DELETED key=" + key;
    } else {
      return clientName + " " + participantName + " DELETE operation failed - consensus failure";
    }
  }

  /**
   * Retrieves a value from the replica's local key-value store.
   *
   * @param key The key to retrieve.
   * @param clientName The client initiating the request.
   * @param participantName The replica name handling the request.
   * @return The value or an error message.
   * @throws RemoteException If an RMI error occurs.
   */
  @Override
  public String get(String key, String clientName, String participantName) throws RemoteException {
    log(clientName + " " + participantName + " received GET request for key=" + key);
    boolean consensus = proposer.propose("GET", key, "");
    if (consensus) {
      String value = localLearner.get(key);
      if (value != null) {
        return participantName + ": Value for key " + key + " is: " + value;
      } else {
        return participantName + ": GET failed for key " + key + " — Key not found";
      }
    } else {
      return clientName + " " + participantName + " GET operation failed - consensus failure";
    }
  }
}
