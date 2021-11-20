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
        Job job = Job.getInstance(config, "mapreduce_q2c");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("/Users/xiexiaohao/IdeaProjects/DataSructure/test1.txt"));
        //TextInputFormat.setMaxInputSplitSize(job, 10176799);
        job.setJarByClass(JobMain.class);

        //String uri = "/Users/xiexiaohao/Desktop/centroids.txt";
        String uri = "/Users/xiexiaohao/Desktop/b3_1/endTest3/part-r-00000";
        //String uri1 = "/Users/xiexiaohao/IdeaProjects/DataSructure/trainLabels.txt";
        //String uri = "/Users/xiexiaohao/Desktop/q2a/endTest2/part-r-00000";
        //DistributedCache.addCacheFile(new URI(uri), config);
        job.addCacheFile(new URI(uri));
        //job.addCacheFile(new URI(uri1));


        job.setMapperClass(KMeansMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

       //job.setCombinerClass(KMeansCombiner.class);

        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("/Users/xiexiaohao/Desktop/b3_1/TestResult"));
        boolean b = job.waitForCompletion(true);

        //--------------------------------------------------

//        Job job1 = Job.getInstance(config, "mapreduce_q2c2");
//        job1.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job1, new Path("/Users/xiexiaohao/IdeaProjects/DataSructure/train5.txt"));
//        //TextInputFormat.setMaxInputSplitSize(job, 10176799);
//        job1.setJarByClass(JobMain.class);
//
//        //String uri = "/Users/xiexiaohao/Desktop/centroids.txt";
//        //String uri = "/Users/xiexiaohao/IdeaProjects/DataSructure/centroid1.txt";
//        //String uri1 = "/Users/xiexiaohao/IdeaProjects/DataSructure/trainLabels.txt";
//        String uri2 = "/Users/xiexiaohao/Desktop/b3_5/endTest/part-r-00000";
//        //DistributedCache.addCacheFile(new URI(uri2), config);
//        job1.addCacheFile(new URI(uri2));
//        //job.addCacheFile(new URI(uri1));
//
//
//        job1.setMapperClass(KMeansMapper.class);
//        job1.setMapOutputKeyClass(Text.class);
//        job1.setMapOutputValueClass(Text.class);
//
//        //job.setCombinerClass(KMeansCombiner.class);
//
//        job1.setReducerClass(KMeansReducer.class);
//        job1.setOutputKeyClass(Text.class);
//        job1.setOutputValueClass(Text.class);
//
//        job1.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setOutputPath(job1, new Path("/Users/xiexiaohao/Desktop/b3_5/endTest2"));
//        boolean b1 = job1.waitForCompletion(true);
//
//        //-------------------------------------------------------------
//
//        Job job2 = Job.getInstance(config, "mapreduce_q2c3");
//        job2.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job2, new Path("/Users/xiexiaohao/IdeaProjects/DataSructure/train5.txt"));
//        //TextInputFormat.setMaxInputSplitSize(job, 10176799);
//        job2.setJarByClass(JobMain.class);
//
//        //String uri = "/Users/xiexiaohao/Desktop/centroids.txt";
//        //String uri = "/Users/xiexiaohao/IdeaProjects/DataSructure/centroid1.txt";
//        //String uri1 = "/Users/xiexiaohao/IdeaProjects/DataSructure/trainLabels.txt";
//        String uri3 = "/Users/xiexiaohao/Desktop/b3_5/endTest2/part-r-00000";
//        //DistributedCache.addCacheFile(new URI(uri3), config);
//        job2.addCacheFile(new URI(uri3));
//        //job.addCacheFile(new URI(uri1));
//
//
//        job2.setMapperClass(KMeansMapper.class);
//        job2.setMapOutputKeyClass(Text.class);
//        job2.setMapOutputValueClass(Text.class);
//
//        //job.setCombinerClass(KMeansCombiner.class);
//
//        job2.setReducerClass(KMeansReducer.class);
//        job2.setOutputKeyClass(Text.class);
//        job2.setOutputValueClass(Text.class);
//
//        job2.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setOutputPath(job2, new Path("/Users/xiexiaohao/Desktop/b3_5/endTest3"));
//        boolean b2 = job2.waitForCompletion(true);

        return b ? 0 : 1;
    }
}
