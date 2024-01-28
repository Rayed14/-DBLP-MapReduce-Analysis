import java.io.IOException;
import java.util.StringTokenizer;

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

public class q1 {
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
		private final static IntWritable one = new IntWritable(1);
		private Text journalYear = new Text();

    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] row = value.toString().split("<");
			// if (row.length > 2 && row[1].equals("article")) 
			if(key.get()!=0)
			{
				// String journal = "";
				// String year = "";
				String journal = row[5];
				String year = row[2];

				String finalJournal = "";
				String finalYear = "";

				for(int i=0;i<year.length();i++){
					if(year.charAt(i) == '.'){
						break;
					}
					if(year.charAt(i) != ',' && year.charAt(i) != '\"' ){
						finalYear += year.charAt(i);
					}
				}

				for(int i=0;i<journal.length();i++){
					if(journal.charAt(i) != ',' && journal.charAt(i) != '\"'){
						finalJournal += journal.charAt(i);
					}
				}

				finalJournal += "-" + finalYear;
				Text Key = new Text(finalJournal); //making key for the reducer function
				context.write(Key,one);

				// for (int i = 2; i < row.length; i++) {
				// 	if (row[i].contains("journal")) {
				// 		journal = row[i].replaceAll("\"", "").trim();
				// 	}
				// 	if (row[i].contains("year")) {
				// 		year = row[i].replaceAll(",", "").replaceAll("\"", "").trim();
				// 	}
				// }
			}
    	}
  	}

	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

    	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			result.set(count);
			context.write(key, result);
		}
  	}

  	public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(q1.class);
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