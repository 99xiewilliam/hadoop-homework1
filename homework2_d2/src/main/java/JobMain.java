import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
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
        TextInputFormat.addInputPath(job1, new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155162650/shakespeare_basket"));
        TextInputFormat.setMaxInputSplitSize(job1, 11090571);

        job1.setMapperClass(HomeWork22DMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(HomeWork22DReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155162650/d1"));

        boolean b1 = job1.waitForCompletion(true);

        Configuration config = super.getConf();
        config.set("fs.defaultFS", "hdfs://dicvmc2.ie.cuhk.edu.hk:8020");
        config.set("mapreduce.framework.name", "yarn");
        config.set("yarn.resourcemanager.hostname", "dicvmc2.ie.cuhk.edu.hk");
        Job job = Job.getInstance(config, "mapreduce_d2");

        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(HomeWork2DMapper.class);
        String uri = "hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155162650/b1/part-r-00000#test";
        DistributedCache.addCacheFile(new URI(uri), config);
        job.addCacheFile(new URI(uri));
        TextInputFormat.addInputPath(job, new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155162650/shakespeare_basket"));
        TextInputFormat.setMaxInputSplitSize(job, 11090571);

        job.setMapperClass(HomeWork2DMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(HomeWork2DReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155162650/d2"));

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
