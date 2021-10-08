import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class MyOutputFormat extends FileOutputFormat {
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(""));
        FSDataOutputStream fsDataOutputStream_bad = fileSystem.create(new Path(""));

        MyRecordWriter myRecordWriter = new MyRecordWriter(fsDataOutputStream, fsDataOutputStream_bad);

        return myRecordWriter;

    }
}
