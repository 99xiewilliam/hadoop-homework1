import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HomeworkD3Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        int len = 0;
        String v3 = null;
        for (Text value : values) {
            String[] split = value.toString().split(",");
            if (split.length == 2) {
                String[] split1 = split[1].split("-");
                if (len < split1.length) {
                    v3 = split[0];
                    len = split1.length;
                }
            }
        }
        if (key.toString().endsWith("2650")) {
            context.write(key, new Text(v3));
        }
    }
}
