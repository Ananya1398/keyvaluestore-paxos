import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * The KeyValueStoreOperations class implements the KeyValueStoreOperation interface
 * to provide basic key-value store operations such as put, get, and delete.
 */
public class KeyValueStoreOperations extends UnicastRemoteObject implements KeyValueStoreOperation {

  /**
   * Constructor for KeyValueStoreOperations.
   */
  protected KeyValueStoreOperations() throws RemoteException {
    super();
  }

  /**
   * Logs a message with a timestamp and thread name
   * @param message The message to log.
   */
  private static void log(String message) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    String timestamp = sdf.format(new Date());
    String fullThreadName = Thread.currentThread().getName();
    System.out.println("[" + timestamp + "] [" + fullThreadName + "] " + message);
  }

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
  @Override
  public boolean put(String key, String value, String clientName, ConcurrentHashMap<String, String> store) throws RemoteException {
    if (store.containsKey(key)) {
      log("Key " + key + " already exists in the key-value store.");
      return false;
    } else {
      store.put(key, value);
      log("Successfully added key: " + key + " with value: " + value);
      return true;
    }
  }

  /**
   * Retrieves the value associated with a key.
   *
   * @param key The key whose value is being retrieved.
   * @param clientName The client performing the operation.
   * @param store ConcurrentHashmap data store instance maintained for every server replica
   * @return A success or failure boolean value.
   * @throws RemoteException If an RMI error occurs.
   */
  @Override
  public boolean get(String key, String clientName, ConcurrentHashMap<String, String> store) throws RemoteException {
    String result = store.get(key);
    if (result == null) {
      log("Key not found: " + key);
      return false;
    } else {
      log("Value for key " + key + ": " + result);
      return true;
    }
  }

  /**
   * Deletes a key-value pair from the store.
   *
   * @param key The key to delete.
   * @param clientName The client performing the operation.
   * @param store ConcurrentHashmap data store instance maintained for every server replica
   * @return A success or failure boolean value.
   * @throws RemoteException If an RMI error occurs.
   */
  @Override
  public boolean delete(String key, String clientName, ConcurrentHashMap<String, String> store) throws RemoteException {
    String deletedValue = store.remove(key);
    if (deletedValue == null) {
      log("Key not found for deletion: " + key);
      return false;
    } else {
      log("Successfully deleted key: " + key);
      return true;
    }
  }
}
