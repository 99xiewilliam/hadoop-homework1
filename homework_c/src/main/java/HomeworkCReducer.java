import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class HomeworkCReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, Integer> map = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//        StringBuffer buffer = new StringBuffer();
        String[] split = null;
        List<String> arr = new ArrayList<>();
        for (Text value : values) {
//            buffer.append(value.toString()).append(",");
            if (value.toString().startsWith(key.toString() + "-")) {
                split = value.toString().split("-");
                if (split.length == 2) {
                    if (!map.containsKey(split[1])) {
                        map.put(split[1], 0);
                    }
                }
            }else {
                arr.add(value.toString());
            }
        }
        if (arr.size() >= 2) {
            int num = map.get(split[1]);
            num++;
            if (split.length == 2) {
                map.put(split[1], num);
            }
        }
//        String[] split = buffer.toString().split("\\|");
//        if (split.length == 2) {
//            String[] split1 = split[0].split("-");
//            if (split1.length == 2) {
//                if (!map.containsKey(split1[1])) {
//                    map.put(split1[1], 0);
//                }
//                String[] split2 = split[1].split(",");
//                if (split2.length >= 2) {
//                    int num = map.get(split1[1]);
//                    num++;
//                    map.put(split1[1], num);
//                }
//            }
//        }
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Set<String> keySet = map.keySet();
        for (String s : keySet) {
            Integer integer = map.get(s);
            context.write(new Text(s), new Text(integer.toString()));
        }
    }
}
