package apress.testhadoop.ad2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserAndIdWritable implements WritableComparable<UserAndIdWritable> {
    private Text User = new Text();
    private Text ID = new Text();

    public UserAndIdWritable(){

    }

    public Text getUser() {
        return User;
    }

    public Text getID() {
        return ID;
    }

    public UserAndIdWritable(Text User, Text ID){
        this.User = User;
        this.ID = ID;
    }

    public void write(DataOutput out) throws IOException {
        this.User.write(out);
        this.ID.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.User.readFields(in);
        this.ID.readFields(in);
    }

    public int compareTo(UserAndIdWritable second){
        if (this.User.toString().equals(second.User.toString())){
            return this.ID.compareTo(second.ID);
        }
        else {
            return this.User.compareTo(second.User);
        }
    }

    public boolean equals(Object o){
        if (!(o instanceof UserAndIdWritable)) {
            return false;
        }
        UserAndIdWritable other = (UserAndIdWritable)o;
        if (this.User.equals(other.User) && this.ID.equals(other.ID)){
            return true;
        }
        else {
            return false;
        }
    }

    public int hashCode(){
        return this.User.hashCode();
    }
}
