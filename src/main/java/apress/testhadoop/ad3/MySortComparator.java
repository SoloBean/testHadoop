package apress.testhadoop.ad3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySortComparator extends WritableComparator {
    public MySortComparator(){
        super(UserAndIdWritable.class, true);
    }

    public int compare(WritableComparable first, WritableComparable second){
        UserAndIdWritable f  = (UserAndIdWritable) first;
        UserAndIdWritable s = (UserAndIdWritable) second;
        if (f.User.toString().equals(s.User.toString())){
            return f.ID.compareTo(s.ID);
        }
        else {
            return f.User.compareTo(s.User);
        }
    }
}
