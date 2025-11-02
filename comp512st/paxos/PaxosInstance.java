package comp512st.paxos;

class PaxosInstance {
    final int sequence;
    int proposalNumber = 0;

    //initialize to negative number so that the comparison will always favour the incoming seq #
    comp512st.paxos.ProposedSeq myProposal = new ProposedSeq(-1, "");
    ProposedSeq promisedProposal = new ProposedSeq(-1, "");
    ProposedSeq acceptedProposal = new ProposedSeq(-1, "");

    Object proposedValue = null;
    Object acceptedValue = null;

    int promiseCount = 0;
    int acceptCount = 0;

    boolean decided = false;

    Object highestAcceptedValue = null;
    ProposedSeq highestAccepted = null;

    public PaxosInstance(int sequence) {
        this.sequence = sequence;
    }
}
