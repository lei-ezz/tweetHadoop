import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The below code is adapted from the Week 10 example code provided to us by our lecturers.
 * It can be found in the following directory: /cs/studres/CS1003/Examples/W10-1-Map-Reduce
 */

public class TweetMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    //a List containing all URLs
    List<String> URLs;

    /**
     * The below method acceots a line from the file(which corresponds to one Tweet) as an argument
     * and uses the JSONObject class to parse it, retrieving the value corresponding to the 'url' key
     * which exists within the 'entities' object at the root level of the JSON object.
     * @param key
     * @param value
     * @param output
     * @throws IOException
     * @throws InterruptedException
     */

    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        String line = value.toString();
        Text x = new Text();

        try {
            //since one line corresponds to one JSON object, you can just read each line individually
            JSONObject obj = new JSONObject(line);
            JSONObject entity = obj.getJSONObject("entities");
            JSONArray urls = entity.getJSONArray("urls");
            URLs = new ArrayList<>();

            for (int i = 0; i < urls.length(); i++) {
                URLs.add(urls.getJSONObject(i).getString("expanded_url"));
            }

            for (String s : URLs) {
                if (!s.equals("null")) {
                    s = "\"" + s + "\"";
                    x.set(s);
                    output.write(x, new LongWritable(1));
                }
            }
        } catch (Exception e) {
        }
    }
}