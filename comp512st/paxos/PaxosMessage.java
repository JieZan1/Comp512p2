package comp512st.paxos;

import java.io.Serializable;

abstract class PaxosMessage implements Serializable {
    final int sequence;

    public PaxosMessage(int sequence) {
        this.sequence = sequence;
    }
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{seq=" + sequence + "}";
    }
}

class PrepareMessage extends PaxosMessage {
    final ProposedSeq proposalNumber;

    public PrepareMessage(int sequence, ProposedSeq proposalNumber) {
        super(sequence);
        this.proposalNumber = proposalNumber;
    }

    @Override
    public String toString() {
        return "PrepareMessage{seq=" + sequence + ", proposal=" + proposalNumber + "}";
    }
}

class PromiseMessage extends PaxosMessage {
    final ProposedSeq proposalNumber;
    final ProposedSeq acceptedProposal;
    final Object acceptedValue;

    public PromiseMessage(int sequence, ProposedSeq proposalNumber,
                   ProposedSeq acceptedProposal, Object acceptedValue) {
        super(sequence);
        this.proposalNumber = proposalNumber;
        this.acceptedProposal = acceptedProposal;
        this.acceptedValue = acceptedValue;

    }

    @Override
    public String toString() {
        return "PromiseMessage{seq=" + sequence +
                ", proposal=" + proposalNumber +
                ", prevAccepted=" +", acceptedProposal=" + acceptedProposal +
                        ", acceptedValue=" + acceptedValue +
                "}";
    }
}

class AcceptMessage extends PaxosMessage {
    final ProposedSeq proposalNumber;
    final Object value;

    public AcceptMessage(int sequence, ProposedSeq proposalNumber, Object value) {
        super(sequence);
        this.proposalNumber = proposalNumber;
        this.value = value;
    }

    @Override
    public String toString() {
        return "AcceptMessage{seq=" + sequence +
                ", proposal=" + proposalNumber +
                ", value=" + value + "}";
    }
}

class AcceptedMessage extends PaxosMessage {
    final ProposedSeq proposalNumber;

    public AcceptedMessage(int sequence, ProposedSeq proposalNumber) {
        super(sequence);
        this.proposalNumber = proposalNumber;
    }

    @Override
    public String toString() {
        return "AcceptedMessage{seq=" + sequence +
                ", proposal=" + proposalNumber + "}";
    }
}

class ConfirmMessage extends PaxosMessage {
    final ProposedSeq proposalNumber;

    public ConfirmMessage(int sequence, ProposedSeq proposalNumber) {
        super(sequence);
        this.proposalNumber = proposalNumber;
    }

    @Override
    public String toString() {
        return "ConfirmMessage{seq=" + sequence +
                ", proposal=" + proposalNumber + "}";
    }
}

class RejectPromiseMessage extends PaxosMessage {
    final ProposedSeq proposalNumber;

    public RejectPromiseMessage(int sequence, ProposedSeq proposalNumber) {
        super(sequence);
        this.proposalNumber = proposalNumber;

    }

    @Override
    public String toString() {
        return "RejectPromiseMessage{seq=" + sequence +
                ", proposal=" + proposalNumber +
                "}";
    }
}

class RejectAcceptMessage extends PaxosMessage {
    final ProposedSeq proposalNumber;

    public RejectAcceptMessage(int sequence, ProposedSeq proposalNumber) {
        super(sequence);
        this.proposalNumber = proposalNumber;

    }

    @Override
    public String toString() {
        return "RejectAcceptMessage{seq=" + sequence +
                ", proposal=" + proposalNumber +
                "}";
    }
}