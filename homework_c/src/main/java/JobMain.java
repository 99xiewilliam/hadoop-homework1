import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool{
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "mapreduce_c");
        job.setInputFormatClass(TextInputFormat.class);
        //TextInputFormat.addInputPath(job, new Path("file:///Users/xiexiaohao/Desktop/mapreduce_c/small"));
        TextInputFormat.addInputPath(job, new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155162650/medium"));

        job.setJarByClass(HomeworkCMapper.class);
        job.setMapperClass(HomeworkCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(HomeworkCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155162650/output_c"));
        //TextOutputFormat.setOutputPath(job, new Path("file:///Users/xiexiaohao/Desktop/mapreduce_c/output"));

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);

        System.exit(run);
    }
}
