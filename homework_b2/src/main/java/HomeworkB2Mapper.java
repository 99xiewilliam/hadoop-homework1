import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HomeworkB2Mapper extends Mapper<LongWritable, Text, FriendBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FriendBean, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        FriendBean friendBean = new FriendBean();
        if (split.length == 2) {
            String[] split1 = split[0].split("-");
            String[] split2 = split[1].split("-");
            if (split1.length == 2) {
                friendBean.setSelf(split1[0]);
                friendBean.setMostFriend(split1[1]);
                if (split2.length == 2) {
                    friendBean.setSimilarity(Double.valueOf(split2[0]));
                    friendBean.setFriendList(split2[1]);
                    context.write(friendBean, value);
                }
            }
        }
    }
}
