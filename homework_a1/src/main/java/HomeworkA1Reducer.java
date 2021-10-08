import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class HomeworkA1Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        StringBuffer buffer = new StringBuffer();

//        int size = Iterables.size()
//        int count = 1;

        for (Text value : values) {
            buffer.append(value.toString()).append("-");
//            if (count != size) {
//                buffer.append(value.toString()).append("-");
//            }else {
//                buffer.append(value.toString());
//            }
        }
        context.write(new Text(buffer.toString().substring(0, buffer.toString().length() - 1)), new Text(key));
    }
}
