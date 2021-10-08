import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FriendBean implements WritableComparable<FriendBean> {
    private Double similarity;
    private String self;
    private String mostFriend;
    private String friendList;

    public void setSimilarity(Double similarity) {
        this.similarity = similarity;
    }

    public void setSelf(String self) {
        this.self = self;
    }

    public void setMostFriend(String mostFriend) {
        this.mostFriend = mostFriend;
    }

    public void setFriendList(String friendList) {
        this.friendList = friendList;
    }

    public Double getSimilarity() {
        return similarity;
    }

    public String getSelf() {
        return self;
    }

    public String getMostFriend() {
        return mostFriend;
    }

    public String getFriendList() {
        return friendList;
    }

    @Override
    public String toString() {
        return self + ":" + mostFriend + "," + "{" + friendList + "}" + "," + similarity;
    }

    @Override
    public int compareTo(FriendBean o) {
        int i = this.self.compareTo(o.self);
        if (i == 0) {
            i = this.similarity.compareTo(o.similarity) * (-1);
        }
        return i;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(self);
        dataOutput.writeUTF(mostFriend);
        dataOutput.writeUTF(friendList);
        dataOutput.writeDouble(similarity);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.self = dataInput.readUTF();
        this.mostFriend = dataInput.readUTF();
        this.friendList = dataInput.readUTF();
        this.similarity = dataInput.readDouble();
    }
}
