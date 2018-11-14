import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondTriangle {
	public static class TokenizerMapper
	extends Mapper<Object, Text, IntWritable[], IntWritable> {
		private IntWritable[] pair = new IntWritable()[2];
		private IntWritable username = new IntWritable();
		private IntWritable follower = new IntWritable();
		private IntWritable empty = new IntWritable(-1);

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				username.set(itr.nextToken());
				if (itr.hasMoreTokens()) {
					follower.set(itr.nextToken());
					//if (follower.compareTo(username) > 0) {
					pair[0].set(username)
					pair[1].set(follower)
					context.write(pair, empty);
					//}
				}
			}
		}
	}

	public static class TokenizerMapperTriplet
	extends Mapper<Object, Text, IntWritable[], IntWritable> {
		private IntWritable[] pair = new IntWritable()[2];
		private IntWritable username = new IntWritable();
		private IntWritable follower1 = new IntWritable();
		private IntWritable follower2 = new IntWritable();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				username.set(itr.nextToken());
				if (itr.hasMoreTokens()) {
					follower1.set(itr.nextToken());
					if (itr.hasMoreTokens()) {
						follower2.set(itr.nextToken());
						if (follower2.compareTo(follower1) > 0) {
							pair[0].set(follower1)
							pair[1].set(follower2)
						} else {
							pair[0].set(follower2)
							pair[1].set(follower1)
						}
						context.write(pair, username);
					}
				}
			}
		}
	}

	public static class TriangleReducer
	extends Reducer<IntWritable[],IntWritable,IntWritable,IntWritable> {
		//private IntWritable[] pair = new IntWritable()[2];
		private IntWritable empty = new IntWritable(-1);
		private IntWritable one = new IntWritable(1);
		private List<IntWritable> allValues = new ArrayList<IntWritable>();

		public void reduce(IntWritable[] key, Iterable<IntWritable> values, Context context
		) throws IOException, InterruptedException {
			boolean isTriangle;
			
			for (IntWritable val : values) {
				if (val.compareTo(empty)==0) {
					isTriangle = true;
				} else {
					allValues.add(val);
				}
			}
			if (isTriangle) {
				for (int i = 0; i < allValues.size(); i++) {
					context.write(allValues.get(i),one);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(new Configuration(), SecondTriangle.class);
		conf.setJobName("second Triangle");
		conf.setJarByClass(SecondTriangle.class);
		FileSystem fs = FileSystem.get(conf);

		MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, TokenizerMapper.class);
		MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, TokenizerMapperTriplet.class);
		conf.setReducerClass(TriangleReducer.class);

		conf.setOutputKeyClass(IntWritable.class); 
		conf.setOutputValueClass(IntWritable.class);

		if (fs.exists(new Path(args[2]))) {
			fs.delete(new Path(args[2]), true);
		}

		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		System.exit(conf.waitForCompletion(true) ? 0 : 1);
	}
}