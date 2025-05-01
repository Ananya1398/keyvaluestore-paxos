import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Client class to interact with the RMI key-value store server.
 */
public class ClientTwo {

  private static final String CLIENT_NAME = "ClientTwo";
  private static ExecutorService executor;
  private static final String SERVER_LIST_FILE = "servers.txt";

  /**
   * Logs a message with a timestamp and thread name.
   *
   * @param message The message to log.
   */
  private static void log(String message) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    String timestamp = sdf.format(new Date());
    String fullThreadName = Thread.currentThread().getName();
    System.out.println("[" + timestamp + "] [" + fullThreadName + "] " + CLIENT_NAME + ": " + message);
  }

  /**
   * Main method to connect the client to a server on user-defined port with hostname specified.
   * Performs data prepopulation and 5 PUTs, 5 GETs and 5 DELETEs.
   */
  public static void main(String[] args) {
    Map<Integer, String> availableServers = getAvailableServers();

    if (args.length < 2) {
      log("Missing arguments. Please use this format: java ClientTwo <hostaddress> <port>");
      return;
    }

    String hostAddress = args[0];
    int port;

    try {
      port = Integer.parseInt(args[1]);
    } catch (NumberFormatException e) {
      log("Invalid port number. Please enter a valid integer for the port.");
      return;
    }

    String replicaName = availableServers.get(port);
    if (replicaName == null) {
      log("Invalid port. No server is registered at port " + port);
      return;
    }

    try {
      Registry registry = LocateRegistry.getRegistry(hostAddress, port);
      ServerReplicaInterface stub = (ServerReplicaInterface) registry.lookup(replicaName);
      log("Connected to RMI server: " + replicaName + " on port: " + port);
      prepopulateData(stub, replicaName);
      executeCommands(stub, replicaName);

      executor = Executors.newFixedThreadPool(2);

      Scanner scanner = new Scanner(System.in);
      log("Enter the operation followed by arguments: (Put key value, Get key, Delete key) or type 'close' to exit client:");

      while (true) {
        String input = scanner.nextLine().trim();

        if (input.equalsIgnoreCase("close")) {
          log("Closing client connection!");
          break;
        }

        executor.submit(() -> {
          try {
            Thread.sleep(new Random().nextInt(1000)); // Random delay
            String response = processCommand(stub, input, replicaName);
            log("Response from server: " + response);
            log("Enter the operation followed by arguments: (Put key value, Get key, Delete key) or type 'close' to exit client:");
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
      }

    } catch (NotBoundException e) {
      log("Server is not running or not bound in the registry.");
    } catch (RemoteException e) {
      log("RemoteException: Connection to the server failed.");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (executor != null) {
        executor.shutdown();
      }
    }
  }

  /**
   * Reads available servers from servers.txt.
   * @return Map of port -> server name.
   */
  private static Map<Integer, String> getAvailableServers() {
    Map<Integer, String> servers = new HashMap<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(SERVER_LIST_FILE))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(" running on port: ");
        if (parts.length == 2) {
          String serverName = parts[0];
          int port = Integer.parseInt(parts[1]);
          servers.put(port, serverName);
        }
      }
    } catch (IOException e) {
      log("Failed to read server list: " + e.getMessage());
    }
    return servers;
  }

  /**
   * Prepopulates the key-value store with initial data.
   * @param stub The Server Replica object.
   */
  private static void prepopulateData(ServerReplicaInterface stub, String replicaName) {
    String[] prepopulatedData = {
            "put 1 panda", "put 2 bird", "put 3 cat", "put 4 dog", "put 5 fish"
    };

    ExecutorService executor = Executors.newFixedThreadPool(2);
    executor.submit(() -> log("Starting data pre-population"));

    for (String operation : prepopulatedData) {
      executor.submit(() -> {
        try {
          Thread.sleep(new Random().nextInt(500)); // Random delay
          String response = processCommand(stub, operation, replicaName);
          log("Response from server for prepopulating data: " + response);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }

    executor.shutdown();
    try {
      if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
    }
  }

  /**
   * Executes a batch of commands
   * @param stub The Server Replica object.
   */
  private static void executeCommands(ServerReplicaInterface stub, String replicaName) {
    String[] executeCommands = {
            "put 6 monkey", "put 7 elephant", "put 8 raccoon", "put 9 swan", "put 10 duck",
            "get 6", "get 7", "get 8", "get 9", "get 10",
            "delete 6", "delete 7", "delete 8", "delete 9", "delete 10"
    };

    ExecutorService executor = Executors.newFixedThreadPool(2);
    executor.submit(() -> log("Starting 5 PUTs, 5 GETs, and 5 DELETEs"));

    for (String operation : executeCommands) {
      executor.submit(() -> {
        try {
          Thread.sleep(new Random().nextInt(1000)); // Random delay for interleaving
          String response = processCommand(stub, operation, replicaName);
          log("Response from server: " + response);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }

    executor.shutdown();
    try {
      if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
    }
  }

  private static String processCommand(ServerReplicaInterface stub, String command, String replicaName) {
    String[] arguments = command.split(" ");
    if (arguments.length < 2) {
      return "Invalid input. Need to provide at least 1 operation and 1 argument.";
    }

    String operation = arguments[0];
    String key = arguments[1];

    try {
      switch (operation.toLowerCase()) {
        case "put":
          if (arguments.length < 3) return "Need key and value for put";
          return stub.put(key, arguments[2], CLIENT_NAME, replicaName);
        case "get":
          return stub.get(key, CLIENT_NAME, replicaName);
        case "delete":
          return stub.delete(key, CLIENT_NAME, replicaName);
        default:
          return "Invalid operation. Use 'put', 'get', or 'delete'";
      }
    } catch (RemoteException e) {
      return "RemoteException: Failed to execute operation - " + command;
    }
  }
}
