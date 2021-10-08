import org.apache.hadoop.mapreduce.Partitioner;

import javax.xml.soap.Text;

public class FriendPartition extends Partitioner<FriendBean, Text> {

    @Override
    public int getPartition(FriendBean friendBean, Text text, int i) {
        System.out.println("123");
        return (friendBean.getSelf().hashCode() & 2147483647) % i;
    }
}
