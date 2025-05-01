package Paxos;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Acceptor in the Paxos consensus algorithm.
 * Responds to prepare and accept requests from proposers.
 * Simulates random failure during prepare or accept phases.
 */
public class Acceptor {
  private final String replicaId;
  private final Map<String, Integer> promisedProposals = new ConcurrentHashMap<>();
  private final Map<String, String> acceptedValues = new ConcurrentHashMap<>();
  private volatile boolean failInPrepare = false;
  private volatile boolean failInAccept = false;

  /**
   * Constructs an Acceptor and starts failure simulation.
   *
   * @param replicaId Unique ID of the server replica.
   */
  public Acceptor(String replicaId) {
    this.replicaId = replicaId;
    simulateFailure();
  }

  /**
   * Randomly simulates failures in prepare or accept phases with 50% probability.
   */
  private void simulateFailure() {
    new Thread(() -> {
      Random rand = new Random();
      while (true) {
        try {
          Thread.sleep(4000);
          if (rand.nextDouble() < 0.5) {
            if (rand.nextBoolean()) {
              failInPrepare = true;
              log("Simulating failure during prepare phase");
            } else {
              failInAccept = true;
              log("Simulating failure during accept phase");
            }
            Thread.sleep(6000 + rand.nextInt(5000));
            failInPrepare = false;
            failInAccept = false;
            log("Acceptor recovered from failure");
          }
        } catch (InterruptedException e) {
          break;
        }
      }
    }).start();
  }

  /**
   * Handles a prepare request from a proposer.
   *
   * @param proposalNumber Proposal number being prepared.
   * @param key Key for which the proposal is made.
   * @return True if promise is made, false if already promised higher.
   */
  public synchronized boolean prepare(int proposalNumber, String key) {
    if (failInPrepare) {
      log("Acceptor simulated failure during prepare");
      return false;
    }
    int currentPromise = promisedProposals.getOrDefault(key, -1);
    log("Received prepare for key=" + key + " with proposal=" + proposalNumber);
    if (proposalNumber > currentPromise) {
      promisedProposals.put(key, proposalNumber);
      log("Promised proposal " + proposalNumber + " for key=" + key);
      return true;
    }
    log("Rejected prepare. Already promised " + currentPromise + " for key=" + key);
    return false;
  }

  /**
   * Handles an accept request from a proposer.
   *
   * @param proposalNumber Proposal number.
   * @param key Key being proposed.
   * @param value Proposed value.
   * @return True if accepted, false if rejected.
   */
  public synchronized boolean accept(int proposalNumber, String key, String value) {
    if (failInAccept) {
      log("Acceptor simulated failure during accept");
      return false;
    }

    int currentPromise = promisedProposals.getOrDefault(key, -1);
    log("Received accept for key=" + key + " proposal=" + proposalNumber + " with value=" + value);
    if (proposalNumber >= currentPromise) {
      promisedProposals.put(key, proposalNumber);
      acceptedValues.put(key, value);
      log("Accepted proposal " + proposalNumber + " with value=" + value + " for key=" + key);
      return true;
    }
    log("Rejected accept. Proposal " + proposalNumber + " < promised " + currentPromise + " for key=" + key);
    return false;
  }

  /**
   * Returns replica ID of this Acceptor.
   */
  public String getReplicaId() {
    return this.replicaId;
  }

  /**
   * Logs a message with timestamp and thread name.
   *
   * @param message Message to be logged.
   */
  private void log(String message) {
    String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
    String threadName = Thread.currentThread().getName();
    System.out.println("[" + timestamp + "] [" + threadName + "] " + replicaId + " [Acceptor]: " + message);
  }
}
