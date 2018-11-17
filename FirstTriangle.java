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

public class FirstTriangle {
	public static class TokenizerMapper
	extends Mapper<Object, Text, LongWritable, LongWritable> {
		private LongWritable username = new LongWritable();
		private LongWritable follower = new LongWritable();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				username.set(Long.parseLong(itr.nextToken()));
				if (itr.hasMoreTokens()) {
					follower.set(Long.parseLong(itr.nextToken()));
					//if (follower.compareTo(username) > 0) {
					context.write(username, follower);
					//}
				}
			}
		}
	}

	public static class TripletReducer
	extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable[]> {
		private LongWritable[] pair = new LongWritable()[2];
		private ArrayList<LongWritable> allConnected = new ArrayList<LongWritable>();
		private LongWritable firstVal = new LongWritable();
		private LongWritable secondVal = new LongWritable();

		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context
		) throws IOException, InterruptedException {
			for (LongWritable val : values) {
				allConnected.add(val);
			}
			if (allConnected.size() > 1) {
				for (int i = 0; i < allConnected.size(); i++) {
					for (int j = (i+1); j < allConnected.size(); j++) {
						firstVal.set(allConnected.get(i).get());
						secondVal.set(allConnected.get(j).get());
						if (firstVal.compareTo(secondVal) < 0) {
							pair[0].set(firstVal.get());
							pair[1].set(secondVal.get());
							context.write(key, pair);
						} else if (firstVal.compareTo(secondVal) > 0) {
							pair[0].set(secondVal.get());
							pair[1].set(firstVal.get());
							context.write(key, pair);
						}
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "13515029 first triangle");
		job.setJarByClass(FirstTriangle.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(TripletReducer.class);
		job.setReducerClass(TripletReducer.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable[].class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}