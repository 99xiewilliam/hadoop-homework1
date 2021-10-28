import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class HomeWork2DReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Map<String, Double> map = new HashMap<>();
    private Integer limited = 40;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        Double count = 0.0;
        for (Text value : values) {
            count += Double.parseDouble(value.toString());
        }
        String str = key.toString();
        map.put(str, count);
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        List<Map.Entry<String, Double>> list = new ArrayList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
            @Override
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                int compare = o1.getValue().compareTo(o2.getValue());
                return -compare;
            }
        });
        Integer i = 0;
        for (Map.Entry<String, Double> entry : list) {
            context.write(new Text(entry.getKey() + " " + entry.getValue().toString()), NullWritable.get());
            i++;
            if (i == limited) {
                break;
            }
        }
        //context.write(new Text(str + " " + count.toString()), NullWritable.get());
    }

}
