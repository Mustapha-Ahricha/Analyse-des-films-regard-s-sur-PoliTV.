import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OurProjet {

    // Mapper Class
    public static class OurProjetMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieId = new Text();
        private Text username = new Text();

        // Map method processes each input record
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Split the CSV record into tokens
            String[] tokens = value.toString().split(",");

            // Assuming the format is "Username, MID, StartTimestamp, EndTimestamp"
            if (tokens.length == 4) {
                String startTimestamp = tokens[2].trim();
                // Check if the start timestamp is in the year 2019
                if (isInYear2019(startTimestamp)) {
                    movieId.set(tokens[1].trim());
                    username.set(tokens[0].trim());
                    // Emit movieId as key and username as value
                    context.write(movieId, username);
                }
            }
        }

        // Check if a given timestamp corresponds to the year 2019
        private boolean isInYear2019(String timestamp) {
            try {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
                Date date = dateFormat.parse(timestamp.split("_")[0]);

                Calendar calendar = Calendar.getInstance();
                calendar.setTime(date);

                int year = calendar.get(Calendar.YEAR);
                return year == 2019;
            } catch (ParseException e) {
                // Handle parsing exception, return false for simplicity
                return false;
            }
        }
    }

    // Reducer Class
    public static class OurProjetReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        // Reduce method processes the grouped key-value pairs
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> uniqueUsers = new HashSet<>();

            // Iterate through values and add unique usernames to the set
            for (Text value : values) {
                uniqueUsers.add(value.toString());
            }

            // If there is only one unique user, emit the key-value pair
            if (uniqueUsers.size() == 1) {
                result.set(uniqueUsers.iterator().next());
                context.write(key, result);
            }
        }
    }

    // Main method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "OurProjetMovies");
        job.setJarByClass(OurProjet.class);
        job.setMapperClass(OurProjetMapper.class);
        job.setReducerClass(OurProjetReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Exit with status 0 on success, 1 on failure
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
