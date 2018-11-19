import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.lang.Long;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendData {
	
	public static class MapperUser
	extends Mapper<Object, Text, LongWritable, LongWritable> {
		private LongWritable username = new LongWritable();
		private LongWritable follower = new LongWritable();
		
		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			StringTokenizer iteration = new StringTokenizer(value.toString());
		while (iteration.hasMoreTokens()) {
			username.set(Long.parseLong(iteration.nextToken()));
			follower.set(Long.parseLong(iteration.nextToken()));
			if (username.compareTo(follower) < 0) {
					context.write(username,follower);
				} else {
					context.write(follower,username);
				}
			}
		}
	}

	public static class ReduceUser
	extends Reducer<LongWritable,LongWritable,Text,Text> {
		
		public void reduce(LongWritable key, Iterable<LongWritable> values,
		Context context
		) throws IOException, InterruptedException {
			ArrayList<LongWritable> followers = new ArrayList<LongWritable>();
			//LongWritable value = new LongWritable();

			for (LongWritable val : values) {
				if (!followers.contains(val)) {
					followers.add(val);
				}
			}
			
			for (LongWritable foll : followers) {
				//value.set(foll);
				context.write(Text(key.get().toString()), Text(foll.get().toString()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "13515029 friend data");
		job.setJarByClass(FriendData.class);
		job.setMapperClass(MapperUser.class);
		job.setCombinerClass(ReduceUser.class);
		job.setReducerClass(ReduceUser.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
