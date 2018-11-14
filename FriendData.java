import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendData {
	
	public static class MapperUser
	extends Mapper<Object, Text, IntWritable, IntWritable> {
		private IntWritable username , follower;
		
		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			StringTokenizer iteration = new StringTokenizer(value.toString()," ");
            while (iteration.hasMoreTokens()) { 
                username = new IntWritable(Integer.parseInt(iteration.nextToken()));
                follower = new IntWritable(Integer.parseInt(iteration.nextToken()));
                if (username.compareTo(follower) < 0) {
                    context.write(username,password);
                }
            }
		}
	}

	public static class ReduceUser
	extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable[]> {
		
		public void reduce(IntWritable key, Iterable<IntWritable> values,
		Context context
		) throws IOException, InterruptedException {			
			for (IntWritable val : values) {
                context.write(key,val);
			}			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "friend data");
		job.setJarByClass(WordCount.class);
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