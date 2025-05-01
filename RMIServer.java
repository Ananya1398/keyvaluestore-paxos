import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;
import java.io.*;

/**
 * The RMIServer class registers ServerReplica objects to the RMI registry,
 * simulating a Paxos-based cluster where each replica can independently participate in consensus.
 */
public class RMIServer {

  private static final String SERVER_LIST_FILE = "servers.txt";
  private static final int num_replicas = 5;
  private static final List<ServerReplica> replicas = new ArrayList<>();

  /**
   * Logs a message with a timestamp and thread name.
   * @param message The message to log.
   */
  private static void log(String message) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    String timestamp = sdf.format(new Date());
    String fullThreadName = Thread.currentThread().getName();
    System.out.println("[" + timestamp + "] [" + fullThreadName + "] " + message);
  }

  /**
   * Main method to start num_replicas ServerReplicas on consecutive ports starting from base port.
   * It also configures the learners and acceptors
   */
  public static void main(String[] args) {
    if (args.length < 1) {
      log("Missing arguments. Please use this format: java RMIServer <basePort>");
      return;
    }

    int port;
    try {
      port = Integer.parseInt(args[0]);
    } catch (NumberFormatException e) {
      log("Invalid port number. Please enter a valid integer for the port.");
      return;
    }

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(SERVER_LIST_FILE, false))) {
      writer.write("");
    } catch (IOException e) {
      log("Failed to clear server list file: " + e.getMessage());
      return;
    }

    for (int i = 0; i < num_replicas; i++) {
      int nextPort = port + i;
      String replicaId = "ServerReplica-" + (i + 1);

      log("Starting " + replicaId + " on port " + nextPort);
      try {
        Registry registry;
        try {
          registry = LocateRegistry.createRegistry(nextPort);
        } catch (RemoteException e) {
          log("Port " + nextPort + " already in use. Trying to bind to existing registry.");
          registry = LocateRegistry.getRegistry(nextPort);
        }

        ServerReplica replica = new ServerReplica(replicaId);
        replicas.add(replica);

        registry.rebind(replicaId, replica);
        writeServerInfo(nextPort, replicaId);
        log(replicaId + " is running on port " + nextPort + " and ready.");
      } catch (Exception e) {
        log("Failed to start " + replicaId + ": " + e.getMessage());
        e.printStackTrace();
      }
    }

    List<Paxos.Acceptor> acceptors = new ArrayList<>();
    List<Paxos.Learner> learners = new ArrayList<>();
    for (ServerReplica r : replicas) {
      acceptors.add(r.getLocalAcceptor());
      learners.add(r.getLocalLearner());
    }

    for (ServerReplica r : replicas) {
      r.configurePaxos(acceptors, learners);
    }

    log("All " + num_replicas + " server replicas started and configured with Paxos.");
    displayAvailableServers();
  }

  /**
   * Writes the server ID and port information to the servers.txt file.
   *
   * @param port The server port number.
   * @param serverId The server identifier.
   */
  private static void writeServerInfo(int port, String serverId) {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(SERVER_LIST_FILE, true))) {
      writer.write(serverId + " running on port: " + port);
      writer.newLine();
      log(serverId + " added to server list on port: " + port);
    } catch (IOException e) {
      log("Failed to write server info to file: " + e.getMessage());
    }
  }

  /**
   * Displays the list of available servers from the file.
   * This helps clients know which ports to connect to.
   */
  private static void displayAvailableServers() {
    log("Available Paxos server replicas:");
    try (BufferedReader reader = new BufferedReader(new FileReader(SERVER_LIST_FILE))) {
      String line;
      while ((line = reader.readLine()) != null) {
        log(line);
      }
    } catch (IOException e) {
      log("Failed to read server list: " + e.getMessage());
    }
  }
}
