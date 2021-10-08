import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
1.继承writableComparator
2。调用父类的有参构造
3。指定分组的规则（重写方法）
 */
public class OrderGroupComparator extends WritableComparator {

    public OrderGroupComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;
        return first.getOrderId().compareTo(second.getOrderId());
    }
}
