import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FriendGroupComparator extends WritableComparator {
    public FriendGroupComparator() {
        super(FriendBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FriendBean first = (FriendBean) a;
        FriendBean second = (FriendBean) b;
        return first.getSelf().compareTo(second.getSelf());
    }
}
