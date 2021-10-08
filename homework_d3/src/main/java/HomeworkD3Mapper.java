import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HomeworkD3Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        if (split.length == 2) {
            String[] split1 = split[0].split("-");
            if (split1.length == 2) {
                String k2 = split1[0];
                String v2 = split1[1] + "," + split[1];
                context.write(new Text(k2), new Text(v2));
            }
        }
    }
}
