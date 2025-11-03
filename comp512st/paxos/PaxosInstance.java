package comp512st.paxos;

// Base class for common state
class PaxosInstance {
    final int sequence;
    boolean decided = false;

    // Acceptor state
    final AcceptorState acceptor;

    // Proposer state (null if this process is not proposing for this instance)
    ProposerState proposer;

    public PaxosInstance(int sequence) {
        this.sequence = sequence;
        this.acceptor = new AcceptorState();
        this.proposer = null; // Created only when needed
    }

    public void initializeAsProposer(ProposedSeq proposal, Object value, Paxos.PendingValue pv) {
        if (proposer == null) {
            proposer = new ProposerState(proposal, value, pv);
            acceptor.promisedProposal = proposal;
        }
    }

    public boolean isProposer() {
        return proposer != null;
    }

    public void abandonProposal() {
        proposer.myPendingValue.rejected = true;
        proposer = null;
    }
}

// Acceptor state - what this process has promised/accepted
class AcceptorState {
    // Initialize to negative number so comparisons always favor incoming proposals
    ProposedSeq promisedProposal = new ProposedSeq(-1, "");
    ProposedSeq acceptedProposal = new ProposedSeq(-1, "");
    Object acceptedValue = null;

    public boolean hasAcceptedValue() {
        return acceptedValue != null;
    }
}

// Proposer state - what this process is proposing
class ProposerState {
    final ProposedSeq myProposal;
    Object proposedValue;

    Paxos.PendingValue myPendingValue;

    // Phase 1 state
    int promiseCount = 0;
    ProposedSeq highestAccepted = null;
    Object highestAcceptedValue = null;

    // Phase 2 state
    int acceptCount = 0;

    public ProposerState(ProposedSeq myProposal, Object proposedValue, Paxos.PendingValue pv) {
        this.myProposal = myProposal;
        this.proposedValue = proposedValue;
        this.myPendingValue = pv;
    }
}