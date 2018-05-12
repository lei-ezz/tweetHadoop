import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountURLsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * This class grabs the number of URLs from the mapper and reduces it so that no URLs are repeated
     * and the number of occurrences are counted
     * @param key
     * @param values
     * @param output
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {

        int occurrences = 0;
        for(LongWritable value : values){
            long l = value.get();
            occurrences += l;
        }
        output.write(key, new LongWritable(occurrences));
    }
}
