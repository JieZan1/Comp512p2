package comp512st.paxos;

import java.io.Serializable;

class ProposedSeq implements Serializable, Comparable<ProposedSeq> {
    final int number;
    final String processId;

    ProposedSeq(int number, String processId) {
        this.number = number;
        this.processId = processId;
    }

    @Override
    public int compareTo(ProposedSeq other) {
        if (this.number != other.number) {
            return Integer.compare(this.number, other.number);
        }

        //Very important, this ensures only one process gets through
        return this.processId.compareTo(other.processId);
    }

    @Override
    public String toString() {
        return processId + ":" + number;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ProposedSeq) {
            ProposedSeq otherSeq = (ProposedSeq) other;
            return this.number == otherSeq.number && this.processId.equals(otherSeq.processId);
        }
        return false;
    }

}
