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
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration config = super.getConf();
//        config.set("fs.defaultFS", "hdfs://dicvmc2.ie.cuhk.edu.hk:8020");
//        config.set("mapreduce.framework.name", "yarn");
//        config.set("yarn.resourcemanager.hostname", "dicvmc2.ie.cuhk.edu.hk");
        Job job = Job.getInstance(config, "mapreduce_q2a");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155162650/trainImage.txt"));
        //TextInputFormat.setMaxInputSplitSize(job, 10176799);
        job.setJarByClass(JobMain.class);

        String uri = "hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155162650/centroids.txt";
        //String uri = "/Users/xiexiaohao/IdeaProjects/DataSructure/centroid1.txt";
        //String uri = "/Users/xiexiaohao/Desktop/q2a/endTest2/part-r-00000";
        //DistributedCache.addCacheFile(new URI(uri), config);
        job.addCacheFile(new URI(uri));

        job.setMapperClass(KMeansMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

       //job.setCombinerClass(KMeansCombiner.class);

        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155162650/q2a"));
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }
}
