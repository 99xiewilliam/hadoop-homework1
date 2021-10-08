package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JobMain extends Configured implements Tool {
    //该方法用于指定一个job任务
    @Override
    public int run(String[] strings) throws Exception {
        //创建一个job对象
        Job job = Job.getInstance(super.getConf(), "wordcount");
        job.setJarByClass(JobMain.class);

        //配置job对象 八个步骤

        //1,指定文件读取方式和路径
        job.setInputFormatClass(TextInputFormat.class);
        //TextInputFormat.addInputPath(job, new Path("hdfs://node01:8080/wordcount"));
        TextInputFormat.addInputPath(job, new Path("file:///Users/xiexiaohao/Desktop/mapreduce/input"));

        //2指定map阶段处理方式
        job.setMapperClass(WordCountMapper.class);
        //map阶段k2 v2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //3 4 5 6
        job.setCombinerClass(MyCombiner.class);

        //7 reducer阶段处理方式和处理类型
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //8 设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出路径
        //TextOutputFormat.setOutputPath(job, new Path("hdfs://"));
        TextOutputFormat.setOutputPath(job, new Path("file:///Users/xiexiaohao/Desktop/mapreduce/output1"));

//        FileSystem fileSystem = FileSystem.get(new URI(""), new Configuration());
//        boolean bl2 = fileSystem.exists(new Path(""));
//        if (bl2) {
//            fileSystem.delete(new Path(""), true);
//        }
        //等待任务结束
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration entries = new Configuration();

        int run = ToolRunner.run(entries, new JobMain(), args);
        System.exit(run);

    }
}
