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
        Job job1 = Job.getInstance(super.getConf(), "mapreduce2_b1");
        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1, new Path("/Users/xiexiaohao/Desktop/test"));

        job1.setJarByClass(JobMain.class);
        job1.setMapperClass(HomeWork21BMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(HomeWork21BReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, new Path("/Users/xiexiaohao/Desktop/b1"));

        boolean b1 = job1.waitForCompletion(true);

        Job job = Job.getInstance(super.getConf(), "mapreduce_b2");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("/Users/xiexiaohao/Desktop/test"));
        job.setJarByClass(JobMain.class);
        //job.addCacheArchive();
        //加载缓存文件
        job.addCacheFile(new URI("/Users/xiexiaohao/Desktop/b1/part-r-00000"));

        job.setMapperClass(HomeWork2BMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(HomeWork2BReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("/Users/xiexiaohao/Desktop/b2"));

        boolean b = job.waitForCompletion(true);


        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
