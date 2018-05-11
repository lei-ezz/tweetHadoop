import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import java.io.IOException;

public class InverseMapper extends Mapper<Text, LongWritable, LongWritable, Text> {

    /*
    This mapper just switches the key and value from the last mapreduce task.
     */
    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        context.write(value, key);
    }
}