import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.lang.Long;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondTriangle {
	public static class TokenizerMapper
	extends Mapper<Text, Text, LongWritable[], LongWritable> {
		private LongWritable[] pair = new LongWritable[2];
		private LongWritable username = new LongWritable();
		private LongWritable follower = new LongWritable();
		private LongWritable empty = new LongWritable(-1);

		public void map(Text key, Text value, Context context
		) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				username.set(Long.parseLong(itr.nextToken()));
				if (itr.hasMoreTokens()) {
					follower.set(Long.parseLong(itr.nextToken()));
					//if (follower.compareTo(username) > 0) {
					pair[0].set(username.get());
					pair[1].set(follower.get());
					context.write(pair, empty);
					//}
				}
			}
		}
	}

	public static class TokenizerMapperTriplet
	extends Mapper<Text, Text, LongWritable[], LongWritable> {
		private LongWritable[] pair = new LongWritable[2];
		private LongWritable username = new LongWritable();
		private LongWritable follower1 = new LongWritable();
		private LongWritable follower2 = new LongWritable();

		public void map(Text key, Text value, Context context
		) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				username.set(Long.parseLong(itr.nextToken()));
				if (itr.hasMoreTokens()) {
					follower1.set(Long.parseLong(itr.nextToken()));
					if (itr.hasMoreTokens()) {
						follower2.set(Long.parseLong(itr.nextToken()));
						//if (follower2.compareTo(follower1) > 0) {
						pair[0].set(follower1.get());
						pair[1].set(follower2.get());
						/*} else {
							pair[0].set(follower2)
							pair[1].set(follower1)
						}*/
						context.write(pair, username);
					}
				}
			}
		}
	}

	public static class TriangleReducer
	extends Reducer<LongWritable[],LongWritable,Text,Text> {
		//private LongWritable[] pair = new LongWritable()[2];
		private LongWritable empty = new LongWritable(-1);
		private LongWritable one = new LongWritable(1);
		private ArrayList<LongWritable> allValues = new ArrayList<LongWritable>();

		public void reduce(LongWritable[] key, Iterable<LongWritable> values, Context context
		) throws IOException, InterruptedException {
			boolean isTriangle = false;
			
			for (LongWritable val : values) {
				if (val.compareTo(empty)==0) {
					isTriangle = true;
				} else {
					allValues.add(val);
				}
			}
			if (isTriangle) {
				long resultKey = 0;
				long resultVal = 0;
				for (int i = 0; i < allValues.size(); i++) {
					resultKey = allValues.get(i).get();
					resultVal = one.get();
					//context.write(allValues.get(i),one);
					context.write(Text(resultKey.toString()),Text(resultVal.toString()));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "13515029 second triangle");
		job.setJarByClass(SecondTriangle.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TokenizerMapperTriplet.class);
		job.setReducerClass(TriangleReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}