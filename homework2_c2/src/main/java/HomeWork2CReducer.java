import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class HomeWork2CReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Integer baskets = 0;
    private Map<Set<String>, Double> map = new HashMap<>();
    private Double min_support = 0.0025;
    private Integer limited = 20;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
//        String[] split = key.toString().split(" ");
//        for (Text value : values) {
//            context.write(new Text(split[0] + " " + split[1] + " " + value.toString()), NullWritable.get());
//        }

        String[] split = key.toString().split(" ");
        Double counter = 0.0;
        String str = key.toString();
        System.out.println("map size");
        System.out.println(map.size());
        if (!str.equals("baskets")) {
            for (Text value : values) {
                counter = counter + 1.0;
            }
            Set<String> set = new HashSet<>(Arrays.asList(split));
            if (!map.containsKey(set)) {
                map.put(set, counter);
            }else {
                map.put(set, map.get(set) + counter);
            }
        }else {
            for (Text value : values) {
                baskets += Integer.parseInt(value.toString());
            }
        }

    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        Map<Set<String>, Double> mapAno = new HashMap<>();

        for (Map.Entry<Set<String>, Double> entry : map.entrySet()) {
            if (entry.getValue() >= min_support * baskets) {
                mapAno.put(entry.getKey(), entry.getValue());
            }
        }
        System.out.println("mapAno size");
        System.out.println(mapAno.size());
        List<Map.Entry<Set<String>, Double>> list = new ArrayList<>(mapAno.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<Set<String>, Double>>() {
            @Override
            public int compare(Map.Entry<Set<String>, Double> o1, Map.Entry<Set<String>, Double> o2) {
                int compare = o1.getValue().compareTo(o2.getValue());
                return -compare;
            }
        });

        int i = 0;
        for (Map.Entry<Set<String>, Double> entry : list) {
            List<String> keyList = new ArrayList<>(entry.getKey());
            context.write(new Text(keyList.get(0) + " " + keyList.get(1) + " " + keyList.get(2) + " " + entry.getValue()), NullWritable.get());
            i++;
            if (i == limited) {
                break;
            }
        }

    }
}
