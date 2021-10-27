import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HomeWork2BMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Double min_support = 0.005;
    private Integer baskets = 0;
    private Map<String, Double> map = new HashMap<>();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        for (String s : split) {
            if (!map.containsKey(s)) {
                map.put(s, 0.0);
            }else {
                map.put(s, map.get(s) + 1.0);
            }
        }
        baskets++;
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Set<String> strings = map.keySet();
        for (String string : strings) {
            if (map.get(string) >= min_support * baskets) {
                context.write(new Text(string), new Text(string + " " + map.get(string).toString()));
            }
        }
        System.out.println("test!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        System.out.println(baskets);
    }
}
