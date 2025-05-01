package Paxos;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Proposer class in the Paxos protocol.
 * Initiates proposals and drives consensus across acceptors.
 */
public class Proposer {
  private final String replicaId;
  private final List<Acceptor> acceptors;
  private final List<Learner> learners;
  private volatile boolean isFailing = false;
  private int proposalSeq = 0;

  /**
   * Constructs a proposer with references to all acceptors and learners.
   * @param replicaId Unique identifier of this proposer.
   * @param acceptors List of all acceptors in the system.
   * @param learners List of all learners in the system.
   */
  public Proposer(String replicaId, List<Acceptor> acceptors, List<Learner> learners) {
    this.replicaId = replicaId;
    this.acceptors = acceptors;
    this.learners = learners;
    simulateFailure();
  }

  /**
   * Runs a full Paxos proposal (prepare, accept, learn).
   * @param operation Operation type (PUT/DELETE/GET).
   * @param key Key involved in the operation.
   * @param value Value to associate with the key.
   * @return True if consensus was reached, false otherwise.
   */
  public boolean propose(String operation, String key, String value) {
    if (isFailing) {
      log("Simulating proposer failure");
      return false;
    }

    int proposalNumber = generateProposalNumber();
    String combinedValue = operation + ":" + key + ":" + value;
    log("Starting Paxos proposal #" + proposalNumber + " for: " + combinedValue);

    int promiseCount = 0;
    for (Acceptor acceptor : acceptors) {
      try {
        boolean promise = acceptor.prepare(proposalNumber, key);
        log("Prepare to " + acceptor.getReplicaId() + " for key=" + key + ": " + (promise ? "PROMISE" : "REJECT"));
        if (promise) promiseCount++;
      } catch (Exception e) {
        log("Prepare failed to " + acceptor.getReplicaId() + ": " + e.getMessage());
      }
    }

    if (promiseCount <= acceptors.size() / 2) {
      log("Not enough promises. Needed >" + (acceptors.size() / 2) + " but got " + promiseCount);
      return false;
    }

    log("Majority promises received. Sending accept requests...");

    int acceptCount = 0;
    for (Acceptor acceptor : acceptors) {
      try {
        boolean accepted = acceptor.accept(proposalNumber, key, combinedValue);
        log("Accept to " + acceptor.getReplicaId() + " for key=" + key + ": " + (accepted ? "ACCEPTED" : "REJECTED"));
        if (accepted) acceptCount++;
      } catch (Exception e) {
        log("Accept failed to " + acceptor.getReplicaId() + ": " + e.getMessage());
      }
    }

    if (acceptCount <= acceptors.size() / 2) {
      log("Not enough accepts. Needed >" + (acceptors.size() / 2) + " but got " + acceptCount);
      return false;
    }

    log("Consensus achieved. Informing learners...");
    for (Learner learner : learners) {
      learner.learn(combinedValue);
    }

    log("Final consensus for proposal #" + proposalNumber + ": " + combinedValue);
    return true;
  }

  /**
   * Randomly simulates proposer failure with probability of 40%.
   */
  private void simulateFailure() {
    new Thread(() -> {
      Random rand = new Random();
      while (true) {
        try {
          Thread.sleep(4000);
          if (rand.nextDouble() < 0.4) {
            isFailing = true;
            log("Proposer simulated failure");
            Thread.sleep(5000 + rand.nextInt(4000)); // 6–10 sec failure
            isFailing = false;
            log("Proposer recovered from failure");
          }
        } catch (InterruptedException e) {
          break;
        }
      }
    }).start();
  }

  /**
   * Generates a unique proposal number using time and replica ID.
   * @return Unique proposal number.
   */
  private int generateProposalNumber() {
    long time = System.currentTimeMillis();
    int uniqueId = Integer.parseInt(replicaId.replaceAll("\\D", ""));
    return (int) (time % Integer.MAX_VALUE) + uniqueId + proposalSeq++;
  }

  /**
   * Logs a timestamped message with thread and replica info.
   * @param message The message to log.
   */
  private void log(String message) {
    String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
    String threadName = Thread.currentThread().getName();
    System.out.println("[" + timestamp + "] [" + threadName + "] " + replicaId + " [Proposer]: " + message);
  }
}
