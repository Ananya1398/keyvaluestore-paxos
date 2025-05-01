import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * ServerReplicaInterface defines the operations that each replica must implement
 * in a Paxos-based distributed key-value store. Each replica is capable of handling
 * PUT, GET, and DELETE operations, with consensus used for write operations.
 */
public interface ServerReplicaInterface extends Remote {

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
  String put(String key, String value, String clientName, String participantName) throws RemoteException;

  /**
   * Retrieves a value from the replica's local key-value store.
   *
   * @param key The key to retrieve.
   * @param clientName The client initiating the request.
   * @param participantName The replica name handling the request.
   * @return The value or an error message.
   * @throws RemoteException If an RMI error occurs.
   */
  String get(String key, String clientName, String participantName) throws RemoteException;

  /**
   * Performs a DELETE operation after reaching consensus using Paxos.
   *
   * @param key The key to delete.
   * @param clientName The client initiating the request.
   * @param participantName The replica name handling the request.
   * @return A success or failure message.
   * @throws RemoteException If an RMI error occurs.
   */
  String delete(String key, String clientName, String participantName) throws RemoteException;
}
