import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class q2 {
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text authorPair = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split("<"); // Splitting values using '<' as separator
            if (key.get()!=0) { // Checking if row contains article
                // String[] authors = row[1].split(","); // Splitting authors using ',' as separator
                String tempAuthor = row[1];
                String cleanAuthor = "";

                for(int i=0;i<tempAuthor.length();i++){ //remove the quotation marks from the data
					if(tempAuthor.charAt(i) != '\"' ){
						tempAuthor.trim();
                        cleanAuthor += tempAuthor.charAt(i); 
					}
				}

                String[] authors = cleanAuthor.split(",");
                // Arrays.sort(authors);

                List<String> sortedAuthors = new ArrayList<String>();
                for (String author : authors) {
                    sortedAuthors.add(author.trim()); // Removing leading/trailing whitespaces
                }
                Collections.sort(sortedAuthors); // Sorting authors alphabetically 
                for (int i = 0; i < sortedAuthors.size(); i++) {
                    for (int j = i + 1; j < sortedAuthors.size(); j++) {
                        // Emitting (key, value) pairs where key is a pair of authors separated by ',' and value is 1
                        context.write(new Text(sortedAuthors.get(i) + "," + sortedAuthors.get(j)), new IntWritable(1));
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            result.set(count);
            context.write(new Text(key.toString().replaceAll("\"", "")), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CoAuthorshipGraph");
        job.setJarByClass(q2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
