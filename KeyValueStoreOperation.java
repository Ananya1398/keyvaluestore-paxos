import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.ConcurrentHashMap;

public interface KeyValueStoreOperation extends Remote {

  /**
   * Stores a key-value pair in the store.
   *
   * @param key The key to store.
   * @param value The value associated with the key.
   * @param clientName The client performing the operation.
   * @param store ConcurrentHashmap data store instance maintained for every server replica
   * @return A success or failure boolean value.
   * @throws RemoteException If an RMI error occurs.
   */
  boolean put(String key, String value, String clientName, ConcurrentHashMap<String, String> store) throws RemoteException;

  /**
   * Retrieves the value associated with a key.
   *
   * @param key The key whose value is being retrieved.
   * @param clientName The client performing the operation.
   * @param store ConcurrentHashmap data store instance maintained for every server replica
   * @return A success or failure boolean value.
   * @throws RemoteException If an RMI error occurs.
   */
  boolean get(String key, String clientName, ConcurrentHashMap<String, String> store) throws RemoteException;

  /**
   * Deletes a key-value pair from the store.
   *
   * @param key The key to delete.
   * @param clientName The client performing the operation.
   * @param store ConcurrentHashmap data store instance maintained for every server replica
   * @return A success or failure boolean value.
   * @throws RemoteException If an RMI error occurs.
   */
  boolean delete(String key, String clientName, ConcurrentHashMap<String, String> store) throws RemoteException;
}
