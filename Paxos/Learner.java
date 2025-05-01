package Paxos;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Learner class in the Paxos protocol.
 * Learns and applies the final consensus value.
 */
public class Learner {
  private final String replicaId;
  private final ConcurrentHashMap<String, String> learnedValues = new ConcurrentHashMap<>();
  private volatile boolean failInLearn = false;

  /**
   * Constructs a learner instance and starts failure simulation.
   * @param replicaId Unique identifier of the replica.
   */
  public Learner(String replicaId) {
    this.replicaId = replicaId;
    simulateFailure();
  }

  /**
   * Randomly simulates learner failure with a probability of 40%.
   */
  private void simulateFailure() {
    new Thread(() -> {
      Random rand = new Random();
      while (true) {
        try {
          Thread.sleep(4000);
          if (rand.nextDouble() < 0.4) {
            failInLearn = true;
            log("Simulating learner failure");
            Thread.sleep(5000 + rand.nextInt(4000));
            failInLearn = false;
            log("Learner recovered from failure");
          }
        } catch (InterruptedException e) {
          break;
        }
      }
    }).start();
  }

  /**
   * Learns and applies the final value if not in failure.
   * @param value The consensus value in format "OPERATION:KEY:VALUE".
   */
  public void learn(String value) {
    if (failInLearn) {
      log("Learner simulated failure");
      return;
    }
    log("Learned value: " + value);
    String[] parts = value.split(":", 3);
    if (parts.length == 3) {
      String operation = parts[0];
      String key = parts[1];
      String val = parts[2];
      if (operation.equals("PUT")) {
        learnedValues.put(key, val);
        log("Applied PUT key=" + key + ", value=" + val);
      } else if (operation.equals("DELETE")) {
        learnedValues.remove(key);
        log("Applied DELETE key=" + key);
      }
    }
  }

  /**
   * Retrieves the learned value for a given key.
   * @param key The key to query.
   * @return The corresponding value, or null if not found.
   */
  public String get(String key) {
    return learnedValues.get(key);
  }

  /**
   * Logs a timestamped message with thread information.
   * @param message The message to log.
   */
  private void log(String message) {
    String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
    String threadName = Thread.currentThread().getName();
    System.out.println("[" + timestamp + "] [" + threadName + "] " + replicaId + " [Learner]: " + message);
  }
}
