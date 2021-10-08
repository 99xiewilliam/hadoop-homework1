import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupMapper extends Mapper<LongWritable, Text, OrderBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, OrderBean, Text>.Context context) throws IOException, InterruptedException {
        //拆分行文本数据，得到订单的ID，订单的金额
        String[] split = value.toString().split("\t");
        //封装OrderBean，得到K2
        OrderBean orderBean = new OrderBean();
        orderBean.setOrderId(split[0]);
        orderBean.setPrice(Double.valueOf(split[2]));
        //将K2和V2写入上下文中
        context.write(orderBean, value);
    }
}
