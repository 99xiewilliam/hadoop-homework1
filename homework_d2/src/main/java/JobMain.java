import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "home_d2");

        job.setInputFormatClass(TextInputFormat.class);
        //hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155162650/medium_relation
        TextInputFormat.setMaxInputSplitSize(job, 350522);
        TextInputFormat.addInputPath(job, new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155162650/output_d1_4/part-r-00000"));

        job.setJarByClass(HomeworkD2Mapper.class);
        job.setMapperClass(HomeworkD2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(4);
        job.setReducerClass(HomeworkD2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://dicvmc2.ie.cuhk.edu.hk:8020/user/s1155162650/output_d2_4"));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        int run = ToolRunner.run(configuration, new JobMain(), args);

        System.exit(run);

    }
}
