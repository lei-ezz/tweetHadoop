import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RetweetReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {
        long occurrences = 0;
        for(LongWritable value : values){
            long l = value.get();
            occurrences = l;
        }
        output.write(key, new LongWritable(occurrences));
    }
}
