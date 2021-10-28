import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job1 = Job.getInstance(super.getConf(), "mapreduce2_d");
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setJarByClass(HomeWork22DMapper.class);
        TextInputFormat.addInputPath(job1, new Path("/Users/xiexiaohao/Desktop/test"));
        //TextInputFormat.setMaxInputSplitSize(job, 11090571);

        job1.setMapperClass(HomeWork22DMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(HomeWork22DReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, new Path("/Users/xiexiaohao/Desktop/d1"));

        boolean b1 = job1.waitForCompletion(true);


        Job job = Job.getInstance(super.getConf(), "mapreduce2_d2");
        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(HomeWork2DMapper.class);
        job.addCacheFile(new URI("/Users/xiexiaohao/Desktop/d1/part-r-00000"));
        TextInputFormat.addInputPath(job, new Path("/Users/xiexiaohao/Desktop/test"));
        //TextInputFormat.setMaxInputSplitSize(job, 11090571);

        job.setMapperClass(HomeWork2DMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(HomeWork2DReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("/Users/xiexiaohao/Desktop/d2"));

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
