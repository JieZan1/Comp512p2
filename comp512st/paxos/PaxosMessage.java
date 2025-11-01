package comp512st.paxos;

import java.io.Serializable;

abstract class PaxosMessage implements Serializable {
    final int sequence;

    PaxosMessage(int sequence) {
        this.sequence = sequence;
    }
}

class PrepareMessage extends PaxosMessage {
    final ProposedSeq proposalNumber;

    PrepareMessage(int sequence, ProposedSeq proposalNumber) {
        super(sequence);
        this.proposalNumber = proposalNumber;
    }
}

class PromiseMessage extends PaxosMessage {
    final ProposedSeq proposalNumber;
    final ProposedSeq acceptedProposal;
    final Object acceptedValue;
    boolean prev_accepted;

    PromiseMessage(int sequence, ProposedSeq proposalNumber,
                   ProposedSeq acceptedProposal, Object acceptedValue) {
        super(sequence);
        this.proposalNumber = proposalNumber;
        this.acceptedProposal = acceptedProposal;
        this.acceptedValue = acceptedValue;

        if (this.acceptedProposal != null) {
            this.prev_accepted = true;
        }
        else{
            this.prev_accepted = false;
        }
    }
    boolean isPrevAccepted() {
        return prev_accepted;
    }
}

class AcceptMessage extends PaxosMessage {
    final ProposedSeq proposalNumber;
    final Object value;

    AcceptMessage(int sequence, ProposedSeq proposalNumber, Object value) {
        super(sequence);
        this.proposalNumber = proposalNumber;
        this.value = value;
    }
}

class AcceptedMessage extends PaxosMessage {
    final ProposedSeq proposalNumber;

    AcceptedMessage(int sequence, ProposedSeq proposalNumber) {
        super(sequence);
        this.proposalNumber = proposalNumber;
    }
}

class ConfirmMessage extends PaxosMessage {
    final ProposedSeq proposalNumber;

    ConfirmMessage(int sequence, ProposedSeq proposalNumber) {
        super(sequence);
        this.proposalNumber = proposalNumber;
    }
}