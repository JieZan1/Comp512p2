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

	// Queue for delivering messages to application in order
	private LinkedBlockingQueue<Object> deliveryQueue = new LinkedBlockingQueue<>();

	// Pending values that haven't been ordered yet
	private ConcurrentHashMap<Integer, PendingValue> pendingValues = new ConcurrentHashMap<>();

	// State for each Paxos instance (sequence number)
	private ConcurrentHashMap<Integer, PaxosInstance> instances = new ConcurrentHashMap<>();

	// For tracking which values have been delivered
	private Set<Integer> deliveredSequences = Collections.synchronizedSet(new HashSet<>());

	private  Set<LabelObj> received_label_obj = Collections.synchronizedSet(new HashSet<>());

	private volatile boolean isShutdown = false;

	// Thread for retrying failed proposals
	private Thread retryThread;

	private int retry_timeout = 200;

	// For synchronization
	private final Object proposalLock = new Object();

	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck)
			throws IOException, UnknownHostException {
		this.failCheck = failCheck;
		this.logger = logger;
		this.myProcess = myProcess;
		this.allGroupProcesses = allGroupProcesses;
		this.allOtherProcesses = Arrays.stream(allGroupProcesses)
				.filter(p -> !p.equals(myProcess))
				.toArray(String[]::new);

		this.MAJORITY = (allGroupProcesses.length / 2) + 1;

		// Initialize GCL with this as the delivery listener
		this.gcl = new GCL(myProcess, allGroupProcesses, this, logger);

		// Start retry thread for failed proposals
		retryThread = new Thread(this::retryPendingValues);
		retryThread.setDaemon(true);
		retryThread.start();

		logger.info("Paxos initialized for process " + myProcess);
	}

	// Application calls this to broadcast a message
	public void broadcastTOMsg(Object val) {
		// block until called

		if (isShutdown) {
			throw new IllegalStateException("Paxos is shutdown");
		}

		LabelObj encapsulated_val = new LabelObj(val, localSeqence.getAndIncrement(), myProcess);

		int seq = currentSequence.get();
		PendingValue pv = new PendingValue(encapsulated_val);
		pendingValues.put(seq, pv);

		logger.fine("Broadcasting value for sequence " + seq + ": " + val);

		// Try to propose this value
		proposeValue(seq, encapsulated_val, pv);

		// Block until this value is accepted by majority
		synchronized (pv) {
			while (!pv.accepted && !isShutdown) {
				try {
					pv.wait();
				} catch (InterruptedException e) {
					logger.log(Level.WARNING, "Interrupted while waiting for acceptance", e);
				}
			}
		}

		logger.fine("Value accepted for sequence " + seq);
	}

	// Application calls this to get next message in order
	public Object acceptTOMsg() throws InterruptedException {
		Object result = null;
		while (!deliveryQueue.isEmpty()) {
			LabelObj val = (LabelObj) deliveryQueue.take();
			if (received_label_obj.contains(val)) {
				continue;
			}
			received_label_obj.add(val);

			result = val.val;
			break;
		}
		return result;

	}


	public void shutdownPaxos() {
		logger.info("Shutting down Paxos");
		isShutdown = true;

		if (retryThread != null) {
			retryThread.interrupt();
		}

		// Wake up any waiting threads
		for (PendingValue pv : pendingValues.values()) {
			synchronized (pv) {
				pv.notifyAll();
			}
		}

		gcl.shutdownGCL();
	}

	// GCL message delivery callback
	//Override to handel
	@Override
	public void deliver(String sender, Object msg) {
		logger.fine("Delivering message " + msg);
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

	private void proposeValue(int seq, Object val, PendingValue pv) {
		if (!instances.containsKey(seq)) {
			instances.put(seq, new PaxosInstance(seq));
		}
		PaxosInstance instance = instances.get(seq);

		synchronized (instance) {
			if (instance.decided) {
				// Already decided, move to next sequence
				if (val.equals(instance.acceptedValue)) {
					markAccepted(pv);
				} else {
					// Try next sequence
					int nextSeq = currentSequence.incrementAndGet();
					pendingValues.put(nextSeq, pv);
					proposeValue(nextSeq, val, pv);
				}
				return;
			}

			// Phase 1a: Prepare - Propose to become leader
			instance.proposalNumber++;
			ProposedSeq pn = new ProposedSeq(instance.proposalNumber, myProcess);
			instance.myProposal = pn;
			instance.promiseCount = 0;
			instance.highestAccepted = null;

			PrepareMessage prepare = new PrepareMessage(seq, pn);

			logger.fine("Sending PREPARE for seq=" + seq + " with proposal=" + pn);
			gcl.multicastMsg(prepare, this.allOtherProcesses);

			failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);
		}
	}

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
		}
	}

	private void handlePrepare(String sender, PrepareMessage msg) {
		failCheck.checkFailure(FailCheck.FailureType.RECEIVEPROPOSE);

		if (!instances.containsKey(msg.sequence)) {
			instances.put(msg.sequence, new PaxosInstance(msg.sequence));
		}
		PaxosInstance instance = instances.get(msg.sequence);

		synchronized (instance) {
			if (msg.proposalNumber.compareTo(instance.promisedProposal) > 0) {
				instance.promisedProposal = msg.proposalNumber;

				PromiseMessage promise = new PromiseMessage(
						msg.sequence,
						msg.proposalNumber,
						instance.acceptedProposal,
						instance.acceptedValue);

				logger.fine("Sending PROMISE to " + sender + " for seq=" + msg.sequence);
				gcl.sendMsg(promise, sender);

				failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
			} else {
				// Send NACK
				logger.fine("Rejecting PREPARE from " + sender + " for seq=" + msg.sequence);
			}
		}
	}

	private void handlePromise(String sender, PromiseMessage msg) {
		PaxosInstance instance = instances.get(msg.sequence);

		// shouldn't happen...
		if (instance == null) {
			logger.fine("Received PROMISE from " + sender + " for unknown seq=" + msg.sequence);
			return;
		}

		synchronized (instance) {
			if (instance.decided) {
				logger.fine("Ignoring PROMISE from " + sender + " for seq=" + msg.sequence + " (already decided)");
				return;
			}

			if (!msg.proposalNumber.equals(instance.myProposal)) {
				logger.fine("Ignoring PROMISE from " + sender + " for seq=" + msg.sequence +
						" (proposal mismatch: got " + msg.proposalNumber + ", expected " + instance.myProposal + ")");
				return;
			}

			instance.promiseCount++;
			logger.fine("Received PROMISE from " + sender + " for seq=" + msg.sequence +
					" (count=" + instance.promiseCount + "/" + MAJORITY + ")");

			if (msg.isPrevAccepted()) {
				// Track highest accepted value
				if (instance.highestAccepted == null ||
						msg.acceptedProposal.compareTo(instance.highestAccepted) > 0) {
					instance.highestAccepted = msg.acceptedProposal;
					instance.highestAcceptedValue = msg.acceptedValue;
					logger.fine("Updating highest accepted value for seq=" + msg.sequence +
							": proposal=" + msg.acceptedProposal + ", value=" + msg.acceptedValue);
				} else {
					logger.fine("Ignoring previously accepted value from " + sender +
							" (proposal " + msg.acceptedProposal + " <= current highest " + instance.highestAccepted + ")");
				}
			}

			// once received promise from the majority
			if (instance.promiseCount >= MAJORITY) {
				logger.fine("Reached majority promises for seq=" + msg.sequence + " (count=" + instance.promiseCount + ")");

				failCheck.checkFailure(FailCheck.FailureType.AFTERBECOMINGLEADER);

				// Phase 2a
				Object valueToPropose = null;
				// adopt to the previously accepted value with highest ballot ID
				if (instance.highestAcceptedValue != null) {
					valueToPropose = instance.highestAcceptedValue;
					logger.fine("Adopting highest accepted value for seq=" + msg.sequence + ": " + valueToPropose);
				} else {
					// Use original proposed value
					PendingValue pv = pendingValues.get(msg.sequence);
					if (pv != null) {
						valueToPropose = pv.value;
						logger.fine("Using original proposed value for seq=" + msg.sequence + ": " + valueToPropose);
					} else {
						logger.fine("No pending value found for seq=" + msg.sequence);
					}
				}

				if (valueToPropose != null) {
					AcceptMessage accept = new AcceptMessage(
							msg.sequence,
							msg.proposalNumber,
							valueToPropose);

					instance.acceptCount = 0;
					instance.proposedValue = valueToPropose;

					logger.fine("Sending ACCEPT for seq=" + msg.sequence + " value=" + valueToPropose);
					gcl.multicastMsg(accept, this.allOtherProcesses);
				} else {
					logger.warning("No value to propose for seq=" + msg.sequence + " despite reaching majority!");
				}
			}
		}
	}

	private void handleAccept(String sender, AcceptMessage msg) {
		if (!instances.containsKey(msg.sequence)) {
			instances.put(msg.sequence, new PaxosInstance(msg.sequence));
		}
		PaxosInstance instance = instances.get(msg.sequence);

		synchronized (instance) {
			if (msg.proposalNumber.compareTo(instance.promisedProposal) >= 0) {
				instance.promisedProposal = msg.proposalNumber;
				instance.acceptedProposal = msg.proposalNumber;
				instance.acceptedValue = msg.value;

				AcceptedMessage accepted = new AcceptedMessage(msg.sequence, msg.proposalNumber);

				logger.fine("Sending ACCEPTED to " + sender + " for seq=" + msg.sequence);
				gcl.sendMsg(accepted, sender);
			}
		}
	}

	private void handleAccepted(String sender, AcceptedMessage msg) {
		PaxosInstance instance = instances.get(msg.sequence);
		if (instance == null){
			return;
		}
		synchronized (instance) {
			if (instance.decided){
				return;
			}

			if (!msg.proposalNumber.equals(instance.myProposal)) {
				return;
			}

			instance.acceptCount++;

			if (instance.acceptCount >= MAJORITY && !instance.decided) {
				instance.decided = true;
				instance.acceptedValue = instance.proposedValue;

				failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);

				logger.info("Consensus reached for seq=" + msg.sequence + " value=" + instance.acceptedValue);

				// Mark pending value as accepted
				PendingValue pv = pendingValues.get(msg.sequence);
				if (pv != null && pv.value.equals(instance.acceptedValue)) {
					markAccepted(pv);
					pendingValues.remove(msg.sequence);
				}

				// Deliver to application
				deliverValue(msg.sequence, instance.acceptedValue);

				ConfirmMessage confirm = new ConfirmMessage(msg.sequence, msg.proposalNumber);

				logger.fine("Sending CONFIRM to " + sender + " for seq=" + msg.sequence);
				gcl.multicastMsg(confirm, this.allOtherProcesses);

				// Move to next sequence
				currentSequence.compareAndSet(msg.sequence, msg.sequence + 1);
			}
		}
	}

	private void handleConfirm(String sender, ConfirmMessage msg) {
		PaxosInstance instance = instances.get(msg.sequence);
		if (instance == null) {
			return;
		}

		synchronized (instance) {
			if (!msg.proposalNumber.equals(instance.acceptedProposal)) {
				return;
			}
			instance.decided = true;

			// Deliver to application
			deliverValue(msg.sequence, instance.acceptedValue);
			// Move to next sequence
			currentSequence.compareAndSet(msg.sequence, msg.sequence + 1);
		}
	}

	private void deliverValue(int seq, Object value) {
		if (deliveredSequences.contains(seq)) {
			return;
		}

		deliveredSequences.add(seq);

		try {
			deliveryQueue.put(value);
			logger.fine("Delivered value for seq=" + seq);
		} catch (InterruptedException e) {
			logger.log(Level.WARNING, "Interrupted while delivering value", e);
		}
	}

	private void markAccepted(PendingValue pv) {
		synchronized (pv) {
			pv.accepted = true;
			pv.notifyAll();
		}
	}

	private void retryPendingValues() {
		while (!isShutdown) {
			try {
				Thread.sleep(this.retry_timeout);

				// Check for pending values that need retry
				for (Map.Entry<Integer, PendingValue> entry : pendingValues.entrySet()) {
					int seq = entry.getKey();
					PendingValue pv = entry.getValue();

					if (!pv.accepted && seq < currentSequence.get()) {
						// This value didn't make it, move to current sequence
						pendingValues.remove(seq);
						int newSeq = currentSequence.getAndIncrement();
						pendingValues.put(newSeq, pv);
						proposeValue(newSeq, pv.value, pv);
					}
				}
			} catch (InterruptedException e) {
				if (!isShutdown) {
					logger.log(Level.WARNING, "Retry thread interrupted", e);
				}
				break;
			}
		}
	}

	// Helper classes
	private static class PendingValue {
		final Object value;
		volatile boolean accepted = false;

		PendingValue(Object value) {
			this.value = value;
		}
	}
}