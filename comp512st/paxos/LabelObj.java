package comp512st.paxos;

import java.io.Serializable;

public class LabelObj implements Serializable {
    Object val;
    final int local_seq;
    String originProcess;


    //Ensure the val is from the same process, also local_seq for non_duplication
    LabelObj(Object val, int local_seq, String originProcess) {
        this.val = val;
        this.local_seq = local_seq;
        this.originProcess = originProcess;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LabelObj)) {
            return false;
        }
        LabelObj other = (LabelObj) o;
        if (local_seq != other.local_seq) {
            return false;
        }
        if (!originProcess.equals(other.originProcess)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return originProcess.hashCode() + local_seq;
    }

    @Override
    public String toString() {
        return originProcess + ":" + local_seq;
    }
}
