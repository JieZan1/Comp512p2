package comp512st.paxos;

import java.io.Serializable;

class ProposedSeq implements Serializable, Comparable<ProposedSeq> {
    final int ballotId;
    final String processId;

    public ProposedSeq(int ballotId, String processId) {
        this.ballotId = ballotId;
        this.processId = processId;
    }

    @Override
    public int compareTo(ProposedSeq other) {
        if (this.ballotId != other.ballotId) {
            return Integer.compare(this.ballotId, other.ballotId);
        }

        return this.processId.compareTo(other.processId);
    }

    @Override
    public String toString() {
        return processId + ":" + ballotId;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ProposedSeq) {
            ProposedSeq otherSeq = (ProposedSeq) other;
            return this.ballotId == otherSeq.ballotId && this.processId.equals(otherSeq.processId);
        }
        return false;
    }

}
