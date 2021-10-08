import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartition extends Partitioner<OrderBean, Text> {
    /**
     * @param orderBean k2
     * @param text v2
     * @param i reduceTask个数
     * @return 返回分区编号
     */
    @Override
    public int getPartition(OrderBean orderBean, Text text, int i) {
        return (orderBean.getOrderId().hashCode() & 2147483647) % i;
    }
}
