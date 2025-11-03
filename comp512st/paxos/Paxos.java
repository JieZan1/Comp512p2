package comp512st.paxos;

import comp512.gcl.*;
import comp512.utils.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;
import java.net.UnknownHostException;

public class Paxos implements GCDeliverListener {
	private GCL gcl;
	private FailCheck failCheck;
	private Logger logger;
	private String myProcess;
	private String[] allGroupProcesses;
	private String[] allOtherProcesses;

	final int MAJORITY ;

	// Sequence number for total ordering
	private AtomicInteger currentSequence = new AtomicInteger(1);

	private AtomicInteger localSeqence = new AtomicInteger(1);

	private AtomicInteger deliveredSeqence = new AtomicInteger(1);

	private AtomicInteger retrySequenceNumber = new AtomicInteger(1);

	// Queue for delivering messages to application in order
	private LinkedBlockingQueue<Object> deliveryQueue = new LinkedBlockingQueue<>();

	// Pending values that haven't been ordered yet
	private volatile PendingValue currentPendingValue = null;

	// State for each Paxos instance (sequence number)
	private ConcurrentHashMap<Integer, PaxosInstance> instances = new ConcurrentHashMap<>();

	// For tracking which values have been delivered

	private ConcurrentHashMap<Integer, Object> deliverBuffer = new ConcurrentHashMap<>();

	private  Set<LabelObj> received_label_obj = Collections.synchronizedSet(new HashSet<>());

	private volatile boolean isShutdown = false;

	// Thread for retrying failed proposals
	private Thread retryThread;

	private int retry_timeout = 200;
    private int currentTimeout = retry_timeout;

	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck)
			throws IOException, UnknownHostException {
		this.failCheck = failCheck;
		this.logger = logger;
		this.myProcess = myProcess;
		this.allGroupProcesses = allGroupProcesses;
		this.allOtherProcesses = Arrays.stream(allGroupProcesses)
				.filter(p -> !p.equals(myProcess))
				.toArray(String[]::new);

		this.MAJORITY = (allGroupProcesses.length / 2);

		// Initialize GCL with this as the delivery listener
		Logger dummyLogger = Logger.getLogger("dummy");
		dummyLogger.setLevel(Level.OFF);

		this.gcl = new GCL(myProcess, allGroupProcesses, this, dummyLogger);

		// Start retry thread for failed proposals
		retryThread = new Thread(this::retryPendingValues);
		retryThread.setDaemon(true);
		retryThread.start();

		logger.info("Paxos initialized for process " + myProcess);
	}

	// Application calls this to broadcast a message
	public void broadcastTOMsg(Object val) {
		if (isShutdown) {
			throw new IllegalStateException("Paxos is shutdown");
		}

		LabelObj encapsulated_val = new LabelObj(val, localSeqence.getAndIncrement(), myProcess);

		int seq = currentSequence.get();
		PendingValue pv = new PendingValue(encapsulated_val);

		currentPendingValue = pv;

		logger.fine("Broadcasting value for sequence " + seq + ": " + val);

		proposeValue(seq, encapsulated_val, pv);

		// Block until this value is accepted by majority
		synchronized (pv) {
			while (!pv.accepted && !isShutdown) {
				try {
					pv.wait();
				} catch (InterruptedException e) {
					logger.log(Level.WARNING, "Unexpected exception while waiting for acceptance", e);
				}
			}
		}

		logger.fine("Value accepted for sequence " + seq);
	}

	public Object acceptTOMsg() throws InterruptedException {
		LabelObj result = null;
		while (true) {
			LabelObj val = (LabelObj) deliveryQueue.take();
			if (received_label_obj.contains(val)) {
				logger.fine("Duplicated obj " + val);
				continue;
			}
			received_label_obj.add(val);

			result = val;
			break;
		}
		logger.fine("Accepted " + result.toString() + "****");

		return result.val;
	}


	public void shutdownPaxos() {
		logger.info("Shutting down Paxos");
		isShutdown = true;

		if (retryThread != null) {
			retryThread.interrupt();
		}

		synchronized (currentPendingValue) {
			currentPendingValue.notifyAll();
		}

		gcl.shutdownGCL();
	}

	// This function is called whenever a GC message is received and read
	@Override
	public void deliver(String sender, Object msg) {
		logger.fine("Receiving message " + msg + "from " + sender);
		if (isShutdown)
			return;
		try {
			if (msg instanceof PaxosMessage) {
				handlePaxosMessage(sender, (PaxosMessage) msg);
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Error handling message from " + sender, e);
		}
	}



	//------------------------------
  	//         Proposer Logic
	//------------------------------

	private void proposeValue(int seq, Object val, PendingValue pv) {
		if (!instances.containsKey(seq)) {
			instances.put(seq, new PaxosInstance(seq));
		}
		PaxosInstance instance = instances.get(seq);

		synchronized (instance) {
			if (instance.decided) {
				if (val.equals(instance.acceptor.acceptedValue)) {
					markAccepted(pv);
				} else {
					int targetSeq = findAvailableSequence();
					pv.rejected = false;
					pv.proposeTime = System.currentTimeMillis();
					proposeValue(targetSeq, val, pv);
				}
				return;
			}

			// Phase 1a
			int proposalNumber = (int) (System.currentTimeMillis() & 0x7FFFFFFF);
			ProposedSeq ps = new ProposedSeq(proposalNumber, myProcess);

			// Initialize as proposer
			instance.initializeAsProposer(ps, val, pv);

			PrepareMessage prepare = new PrepareMessage(seq, ps);

			logger.fine("Sending PREPARE for seq=" + seq + " with proposal=" + ps);
			gcl.multicastMsg(prepare, this.allOtherProcesses);

			failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);
		}
	}

	private void handlePromise(String sender, PromiseMessage msg) {
		PaxosInstance instance = instances.get(msg.sequence);

		if (instance == null || !instance.isProposer()) {
			logger.fine("Received PROMISE but not proposing for seq=" + msg.sequence);
			return;
		}

		synchronized (instance) {
			if (instance.decided) {
				Object valueToPropose = instance.acceptor.acceptedValue;

				AcceptMessage accept = new AcceptMessage(
						msg.sequence,
						msg.proposalNumber,
						valueToPropose
				);
				gcl.multicastMsg(accept, this.allOtherProcesses);
				return;
			}

			if (msg.proposalNumber.compareTo(instance.proposer.myProposal) < 0) {
				logger.fine("Ignoring PROMISE from " + sender + " for seq=" + msg.sequence +
						" (lower ballot id)");
				return;
			}

			instance.proposer.promiseCount++;
			logger.fine("Received PROMISE from " + sender + " for seq=" + msg.sequence +
					" (count=" + instance.proposer.promiseCount + "/" + MAJORITY + ")");

			if (msg.acceptedValue != null) {
				if (instance.proposer.highestAccepted == null ||
						msg.acceptedProposal.compareTo(instance.proposer.highestAccepted) > 0) {

					instance.proposer.highestAccepted = msg.acceptedProposal;
					instance.proposer.highestAcceptedValue = msg.acceptedValue;
					logger.fine("Updating highest accepted value for seq=" + msg.sequence);
				}
			}

			// Once received promise from the majority
			if (instance.proposer.promiseCount >= MAJORITY) {
				logger.fine("Reached majority promises for seq=" + msg.sequence);

				failCheck.checkFailure(FailCheck.FailureType.AFTERBECOMINGLEADER);

				// Phase 2a - adopt highest accepted value or use our own
				Object valueToPropose = instance.proposer.highestAcceptedValue != null
						? instance.proposer.highestAcceptedValue
						: instance.proposer.proposedValue;

				if (valueToPropose != null) {
					AcceptMessage accept = new AcceptMessage(
							msg.sequence,
							msg.proposalNumber,
							valueToPropose
					);

					instance.proposer.acceptCount = 0;
					instance.proposer.proposedValue = valueToPropose;
					// As an acceptor, accept our own proposal
					instance.acceptor.promisedProposal = msg.proposalNumber;
					instance.acceptor.acceptedProposal = msg.proposalNumber;
					instance.acceptor.acceptedValue = valueToPropose;

					logger.fine("Sending ACCEPT for seq=" + msg.sequence);
					gcl.multicastMsg(accept, this.allOtherProcesses);
				}
			}
		}
	}

	private void handleAccepted(String sender, AcceptedMessage msg) {
		PaxosInstance instance = instances.get(msg.sequence);

		if (instance == null || !instance.isProposer()) {
			return;
		}

		synchronized (instance) {
			if (instance.decided) {
				if (msg.proposalNumber.equals(instance.acceptor.acceptedProposal)) {
					logger.info("Resending CONFIRM to " + sender + " for seq=" + msg.sequence +
							" (already decided)");
					ConfirmMessage confirm = new ConfirmMessage(msg.sequence, msg.proposalNumber);
					gcl.sendMsg(confirm, sender);
				} else {
					logger.fine("Ignoring PROMISE from " + sender + " for seq=" + msg.sequence +
							" (already decided with different proposal)");
				}
				return;
			}

			if (!msg.proposalNumber.equals(instance.proposer.myProposal)) {
				return;
			}

			instance.proposer.acceptCount++;

			if (instance.proposer.acceptCount >= MAJORITY && !instance.decided) {
				instance.decided = true;

				failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);

				logger.info("Consensus reached for seq=" + msg.sequence +
						" value=" + instance.proposer.proposedValue);

				// Mark pending value as accepted
				PendingValue pv = instance.proposer.myPendingValue;  // Get from proposer state
				if (pv != null && pv.value.equals(instance.proposer.proposedValue)) {
					markAccepted(pv);
				}

				deliverValue(msg.sequence, instance.proposer.proposedValue);

                logger.info("Broadcasting CONFIRM for seq=" + msg.sequence);
				ConfirmMessage confirm = new ConfirmMessage(msg.sequence, msg.proposalNumber);
				gcl.multicastMsg(confirm, this.allOtherProcesses);

				currentSequence.compareAndSet(msg.sequence, msg.sequence + 1);
			}
		}
	}

	private void handleRejectPromise(String sender, RejectPromiseMessage msg){
		PaxosInstance instance = instances.computeIfAbsent(msg.sequence, PaxosInstance::new);
		if (instance.isProposer()) {
			logger.info("Detected higher proposal " + msg.proposalNumber +
					" for seq=" + msg.sequence + ". Abandoning my proposal.");
			instance.abandonProposal();
		}
	}

	private void handleRejectAccept(String sender, RejectAcceptMessage msg ){
		PaxosInstance instance = instances.computeIfAbsent(msg.sequence, PaxosInstance::new);
		if (instance.isProposer()) {
			logger.info("Detected higher proposal " + msg.proposalNumber +
					" for seq=" + msg.sequence + ". Abandoning my proposal.");
			instance.abandonProposal();
		}
	}


	//------------------------------
	//         Acceptor Logic
	//----------------------------


	private void handlePrepare(String sender, PrepareMessage msg) {
		failCheck.checkFailure(FailCheck.FailureType.RECEIVEPROPOSE);

		PaxosInstance instance = instances.computeIfAbsent(msg.sequence, PaxosInstance::new);

		synchronized (instance) {
			// Check if we're the proposer and this proposal supersedes ours

			if (instance.decided) {
				logger.info("Sending PROMISE with decided value to " + sender + " for seq=" + msg.sequence);
				PromiseMessage promise = new PromiseMessage(
						msg.sequence,
						msg.proposalNumber,
						instance.acceptor.acceptedProposal,
						instance.acceptor.acceptedValue
				);
				gcl.sendMsg(promise, sender);
				return;
			}

			if (instance.isProposer() &&
					msg.proposalNumber.compareTo(instance.proposer.myProposal) > 0) {

				logger.info("Detected higher proposal " + msg.proposalNumber +
						" for seq=" + msg.sequence + ". Abandoning my proposal.");
				instance.abandonProposal();
			}

			// As an acceptor, promise to the higher proposal
			if (msg.proposalNumber.compareTo(instance.acceptor.promisedProposal) > 0) {
				instance.acceptor.promisedProposal = msg.proposalNumber;

				PromiseMessage promise = new PromiseMessage(
						msg.sequence,
						msg.proposalNumber,
						instance.acceptor.acceptedProposal, //ProposedSeq acceptedProposal = new ProposedSeq(-1, "");
						instance.acceptor.acceptedValue  //Object acceptedValue = null;
				);

				logger.fine("Sending PROMISE to " + sender + " for seq=" + msg.sequence);
				gcl.sendMsg(promise, sender);

				failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
			} else {
//				RejectPromiseMessage reject = new RejectPromiseMessage(msg.sequence, instance.acceptor.promisedProposal);
//				gcl.sendMsg(reject, sender);
				logger.fine("Rejecting PREPARE from " + sender + " for seq=" + msg.sequence);
			}
		}
	}


	private void handleAccept(String sender, AcceptMessage msg) {
		PaxosInstance instance = instances.computeIfAbsent(msg.sequence, PaxosInstance::new);

		synchronized (instance) {
			// Check if we're the proposer and this accept supersedes ours
			if (instance.isProposer() &&
					msg.proposalNumber.compareTo(instance.proposer.myProposal) > 0) {

				logger.info("Detected higher accept proposal for seq=" + msg.sequence);
				instance.abandonProposal();
			}

			if (instance.decided) {
				if (msg.value.equals(instance.acceptor.acceptedValue)) {
					logger.info("Sending ACCEPTED for already decided value to " + sender +
							" for seq=" + msg.sequence);
					AcceptedMessage accepted = new AcceptedMessage(msg.sequence, msg.proposalNumber);
					gcl.sendMsg(accepted, sender);
				} else {
					logger.fine("Ignoring ACCEPT from " + sender + " for seq=" + msg.sequence +
							" (already decided with different value)");
				}
				return;
			}

			if (msg.proposalNumber.compareTo(instance.acceptor.promisedProposal) >= 0) {
				instance.acceptor.promisedProposal = msg.proposalNumber;
				instance.acceptor.acceptedProposal = msg.proposalNumber;
				instance.acceptor.acceptedValue = msg.value;

				AcceptedMessage accepted = new AcceptedMessage(msg.sequence, msg.proposalNumber);

				logger.fine("Sending ACCEPTED to " + sender + " for seq=" + msg.sequence);
				gcl.sendMsg(accepted, sender);
			}
			else {
//				RejectAcceptMessage reject = new RejectAcceptMessage(msg.sequence, msg.proposalNumber);
//				gcl.sendMsg(reject, sender);
				logger.fine("Rejecting PREPARE from " + sender + " for seq=" + msg.sequence);
			}
		}
	}



	private void handleConfirm(String sender, ConfirmMessage msg) {
        logger.fine("Entered handleConfirm from " + sender + " for seq=" + msg.sequence);
		PaxosInstance instance = instances.get(msg.sequence);
		if (instance == null) {
            logger.fine("handleConfirm: instance == null for seq=" + msg.sequence);
			return;
		}

		synchronized (instance) {
			if (!msg.proposalNumber.equals(instance.acceptor.acceptedProposal)) {
				logger.fine("Received Wrong confirm from " + sender + " for seq=" + msg.sequence + "||" + msg.proposalNumber + " vs. " + instance.acceptor.acceptedProposal);
				return;
			}

			if (instance.decided) {
				logger.fine("Received Stale confirm from " + sender + " for seq=" + msg.sequence);
				return;
			}

			logger.fine("Decided on Confirm message from " + sender + " for seq=" + msg.sequence);

			instance.decided = true;

			deliverValue(msg.sequence, instance.acceptor.acceptedValue);

			currentSequence.compareAndSet(msg.sequence, msg.sequence + 1);
		}
	}

	//------------------------
 	//        Deliver / retry
 	//------------------------

	private void handlePaxosMessage(String sender, PaxosMessage msg) {
		if (msg instanceof PrepareMessage) {
			handlePrepare(sender, (PrepareMessage) msg);
		} else if (msg instanceof PromiseMessage) {
			handlePromise(sender, (PromiseMessage) msg);
		} else if (msg instanceof AcceptMessage) {
			handleAccept(sender, (AcceptMessage) msg);
		} else if (msg instanceof AcceptedMessage) {
			handleAccepted(sender, (AcceptedMessage) msg);
		} else if (msg instanceof ConfirmMessage){
			handleConfirm(sender, (ConfirmMessage) msg);
		} else if (msg instanceof RejectPromiseMessage) {
			handleRejectPromise(sender, (RejectPromiseMessage) msg);
		} else if (msg instanceof RejectAcceptMessage) {
			handleRejectAccept(sender, (RejectAcceptMessage) msg);
		}
	}

	private synchronized void deliverValue(int seq, Object value) {
        LabelObj labelVal = (LabelObj)value;
        Object[] info  = (Object[]) labelVal.val;
		logger.info("deliverValue ENTRY: seq=" + seq +
				", value=" + value +
				", currentDeliveredSeq=" + deliveredSeqence.get() +
                ", move=" + Arrays.toString(info) + 
				", bufferSize=" + deliverBuffer.size());

		// Use putIfAbsent to avoid race condition
		Object existingValue = deliverBuffer.putIfAbsent(seq, value);
		if (existingValue != null) {
			// Value already exists for this sequence
			logger.warning("deliverValue DUPLICATE: seq=" + seq +
					" already exists in buffer with value=" + existingValue);
			return;
		}

		logger.fine("deliverValue BUFFERED: seq=" + seq + " added to buffer");

		// Synchronize on deliveredSequence to ensure only one thread delivers at a time
		try {
			int deliveryCount = 0;
			int expectedSeq = deliveredSeqence.get();

			logger.fine("deliverValue LOOP START: expecting seq=" + expectedSeq +
					", buffer contains keys=" + deliverBuffer.keySet());

			while (deliverBuffer.containsKey(deliveredSeqence.get())) {
				int currentSeq = deliveredSeqence.get();

				logger.fine("deliverValue LOOP ITERATION: attempting to deliver seq=" + currentSeq);

				Object val = deliverBuffer.remove(currentSeq);

				if (val != null) {
					logger.info("deliverValue DELIVERING: seq=" + currentSeq +
							", value=" + val +
                            ", move2=" + Arrays.toString(info) +
							", queueSize=" + deliveryQueue.size());

					deliveryQueue.put(val);

					logger.info("deliverValue QUEUED: seq=" + currentSeq +
							" successfully added to deliveryQueue");

					int newSeq = deliveredSeqence.incrementAndGet();
					deliveryCount++;

					logger.fine("deliverValue INCREMENTED: deliveredSeq now=" + newSeq);
				} else {
					logger.warning("deliverValue NULL VALUE: seq=" + currentSeq +
							" was in buffer but value was null");
					// This shouldn't happen, but increment anyway to avoid infinite loop
					deliveredSeqence.incrementAndGet();
				}
			}

			logger.info("deliverValue LOOP END: delivered " + deliveryCount +
					" messages, deliveredSeq now=" + deliveredSeqence.get() +
					", remaining buffer keys=" + deliverBuffer.keySet());

			logger.fine("deliverValue COMPLETE: seq=" + seq);

		} catch (InterruptedException e) {
			Thread.currentThread().interrupt(); // Restore interrupt status
			logger.log(Level.SEVERE, "deliverValue INTERRUPTED: seq=" + seq +
					", deliveredSeq=" + deliveredSeqence.get() +
					", bufferSize=" + deliverBuffer.size(), e);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "deliverValue UNEXPECTED ERROR: seq=" + seq +
					", deliveredSeq=" + deliveredSeqence.get(), e);
		}

		logger.info("deliverValue EXIT: seq=" + seq +
				", deliveredSeq=" + deliveredSeqence.get() +
				", bufferSize=" + deliverBuffer.size());
	}

	private void markAccepted(PendingValue pv) {
		synchronized (pv) {
			pv.accepted = true;
			pv.notifyAll();
		}
	}

	private void retryPendingValues() {
		currentTimeout = this.retry_timeout;
		final int MAX_TIMEOUT = 10000;
		final double BACKOFF_MULTIPLIER = 1.2;

		while (!isShutdown) {
			try {
				Thread.sleep(100);

				boolean foundPendingValue = false;

				PendingValue pv = currentPendingValue;
				if (pv != null) {
					synchronized (pv) {
						long currentTime = System.currentTimeMillis();
						boolean isTimedOut = (currentTime - pv.proposeTime) > currentTimeout;

						if (!pv.accepted && (pv.rejected || isTimedOut)) {
							foundPendingValue = true;
							int targetSeq = findAvailableSequence();
							pv.rejected = false;
							pv.proposeTime = System.currentTimeMillis();

							logger.info("Retrying proposal at seq=" + targetSeq);
							proposeValue(targetSeq, pv.value, pv);
						}
					}
				}

				// Increase timeout if we had retries, reset if idle
				if (foundPendingValue) {
					currentTimeout = Math.min((int)(currentTimeout * BACKOFF_MULTIPLIER), MAX_TIMEOUT);
					logger.fine("Increased retry timeout to " + currentTimeout + "ms");
				} else {
					currentTimeout = this.retry_timeout;
				}

			} catch (InterruptedException e) {
				if (!isShutdown) {
					logger.log(Level.WARNING, "Retry thread interrupted", e);
				}
				break;
			} catch (Exception e) {
				logger.log(Level.WARNING, "Error in retry thread", e);
			}
		}
	}

	/**
	 * Find an available sequence number by looking for gaps in instance keys.
	 * If no gap found, return highest key + 1.
	 */
	private int findAvailableSequence() {
		synchronized (instances) {
			if (instances.isEmpty()) {
				return 1;
			}

			// Get all instance keys and sort them
			List<Integer> keys = new ArrayList<>(instances.keySet());
			Collections.sort(keys);

			// Look for gaps in the sequence
			int expectedSeq = keys.get(0);
			for (int key : keys) {
				if (key > expectedSeq) {
					// Found a gap
					logger.info("Found gap at sequence " + expectedSeq);
					return expectedSeq;
				}
				expectedSeq = key + 1;
			}

			// No gap found, return highest + 1
			int nextSeq = keys.get(keys.size() - 1) + 1;
			logger.info("No gap found, using sequence " + nextSeq);
			return nextSeq;
		}
	}

	// Helper classes
	class PendingValue {
		final Object value;
		volatile boolean accepted = false;
		volatile boolean rejected = false;
		volatile long proposeTime; // Track when this value was proposed

		PendingValue(Object value) {
			this.value = value;
			this.proposeTime = System.currentTimeMillis();
		}
	}
}