import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;

/**
 * The below code is adapted from the Week 10 example code provided to us by our lecturers.
 * It can be found in the following directory: /cs/studres/CS1003/Examples/W10-1-Map-Reduce
 */

public class RetweetMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    String ID;
    int retweets;

    /**
     * The below method accepts a line from the file(which corresponds to one Tweet) as an argument
     * and uses the JSONObject class to parse it.
     * @param key
     * @param value
     * @param output
     * @throws IOException
     * @throws InterruptedException
     */

    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        String line = value.toString();

        try {
            //since one line corresponds to one JSON object, you can just read each line individually
            JSONObject obj = new JSONObject(line);
            ID = "http://twitter.com/user/status/" + obj.getString("id");
            JSONObject retweeted_status = obj.getJSONObject("retweeted_status");
            //grab number of retweets
            retweets = retweeted_status.getInt(("retweet_count"));
            output.write(new Text(ID), new LongWritable(retweets));
            ID = "";
            retweets = 0;
        } catch (Exception e) {
        }
    }
}