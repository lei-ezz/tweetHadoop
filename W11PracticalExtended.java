import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.commons.io.FileUtils;


import java.io.File;
import java.io.IOException;

public class W11PracticalExtended {
    public static void main(String[] args) {

        //Check number of arguments to ensure they are correct
        if (args.length != 3) {
            System.out.println("Usage: java -cp \"lib/*:bin\" W11Practical <input_path> <output_directory> <job>");
            System.exit(1);
        }

        //Set the home directory of the hadoop library to the root
        System.setProperty("hadoop.home.dir", "/");

        //Grab the arguments that are input
        String inputFile = args[0];
        String outputFile = args[1];

        //Set the configuration and job
        Configuration conf = new Configuration();
        Job urlCountJob;
        Job sortByOccurrenceJob;

        //The below method is at risk of throwing three different exceptions
        try {

            //---------------------------------------------------------------------------------
            //    JOB 1: Collect Info from JSON Files, Generate Classic MapReduce Results
            //---------------------------------------------------------------------------------

            //name the job
            urlCountJob = Job.getInstance(conf, "URL Count");

            //set the input and output paths
            FileInputFormat.setInputPaths(urlCountJob, new Path(inputFile));
            FileOutputFormat.setOutputPath(urlCountJob, new Path(outputFile + "-temp"));
            urlCountJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            //set the mapper
            switch(args[2]) {
                case "1":
                    urlCountJob.setMapperClass(TweetMapper.class);
                    // The output of the reducer is a map from unique words to their total counts.
                    urlCountJob.setReducerClass(CountURLsReducer.class);
                break;

                case "2":
                    urlCountJob.setMapperClass(RetweetMapper.class);
                    urlCountJob.setReducerClass(RetweetReduce.class);
                break;
            }

            //set the key and value type of the job -- in this case,
            //the key is the expanded_url and the value is the count
            urlCountJob.setMapOutputKeyClass(Text.class);
            urlCountJob.setMapOutputValueClass(LongWritable.class);

            // Specify the output types produced by reducer (words with total counts)
            urlCountJob.setOutputKeyClass(Text.class);
            urlCountJob.setOutputValueClass(LongWritable.class);


            if (!urlCountJob.waitForCompletion(true)) {
                System.exit(1);
            }

            //---------------------------------------------------------------------------------
            //    JOB 2: Invert Key and Value, Sort by Key and Value
            //---------------------------------------------------------------------------------

            //Name the Job and initialize it
            sortByOccurrenceJob = Job.getInstance(conf, "Occurrence Frequency");

            //Set the inversemapper class to be the new mapper -- this
            //class flips keys and values and re-maps them
            sortByOccurrenceJob.setMapperClass(InverseMapper.class);

            //Sets the number of reduce tasks to one so that the below sortcomparator class
            //can perform it's sort, by sorting them in descending order
            sortByOccurrenceJob.setNumReduceTasks(1);
            sortByOccurrenceJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            //The output types are switched now, long then text instead of text then long
            sortByOccurrenceJob.setOutputKeyClass(LongWritable.class);
            sortByOccurrenceJob.setOutputValueClass(Text.class);

            //accepts a sequence file, which is a file specially encoded to allow for
            //chained MapReduce jobs
            sortByOccurrenceJob.setInputFormatClass(SequenceFileInputFormat.class);

            //reads from the temp file and outputs to the final file
            FileInputFormat.addInputPath(sortByOccurrenceJob, new Path(outputFile + "-temp"));
            FileOutputFormat.setOutputPath(sortByOccurrenceJob, new Path(outputFile));

            if (!sortByOccurrenceJob.waitForCompletion(true)) {
                System.exit(1);
            //the below line deletes the 'temp' directory after it's been used to create the sorted map
            } else FileUtils.deleteDirectory(new File(outputFile + "-temp"));

        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
}