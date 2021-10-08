import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class HomeworkE2Reducer extends Reducer<FriendBean, Text, Text, NullWritable> {

    @Override
    protected void reduce(FriendBean key, Iterable<Text> values, Reducer<FriendBean, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        int bk = 0;
        for (Text value : values) {
            if (key.getSelf().endsWith("2650")) {
                context.write(new Text(key.toString()), NullWritable.get());
            }
            bk++;
            if ( bk >=3 ){
                break;
            }
        }
    }
}
