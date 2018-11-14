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

public class FirstTriangle {
	public static class TokenizerMapper
	extends Mapper<Object, Text, IntWritable, IntWritable> {
		private IntWritable username = new IntWritable();
		private IntWritable follower = new IntWritable();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				username.set(itr.nextToken());
				if (itr.hasMoreTokens()) {
					follower.set(itr.nextToken());
					if (follower.compareTo(username) > 0) {
						context.write(username, follower);
					}
				}
			}
		}
	}

	public static class TripletReducer
	extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable[]> {
		private IntWritable[] pair = new IntWritable()[2];
		private List<IntWritable> allConnected = new ArrayList<IntWritable>();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context
		) throws IOException, InterruptedException {
			for (IntWritable val : values) {
				allConnected.add(val);
			}
			if (allConnected.size() > 1) {
				for (int i = 0; i < allConnected.size(); i++) {
					for (int j = 0; j < allConnected.size(); j++) {
						firstVal = allConnected.get(i);
						secondVal = allConnected.get(j);
						if (firstVal.compareTo(secondVal) != 0) {
							pair[0].set(allConnected.get(i));
							pair[1].set(allConnected.get(j));
							context.write(key, pair);
						}
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "first triangle");
		job.setJarByClass(FirstTriangle.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(TripletReducer.class);
		job.setReducerClass(TripletReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable[].class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}