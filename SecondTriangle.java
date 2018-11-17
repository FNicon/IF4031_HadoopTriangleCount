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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondTriangle {
	public static class TokenizerMapper
	extends Mapper<Object, Text, LongWritable[], LongWritable> {
		private LongWritable[] pair = new LongWritable()[2];
		private LongWritable username = new LongWritable();
		private LongWritable follower = new LongWritable();
		private LongWritable empty = new LongWritable(-1);

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				username.set(Long.parseLong(itr.nextToken()));
				if (itr.hasMoreTokens()) {
					follower.set(Long.parseLong(itr.nextToken()));
					//if (follower.compareTo(username) > 0) {
					pair[0].set(username.get())
					pair[1].set(follower.get())
					context.write(pair, empty);
					//}
				}
			}
		}
	}

	public static class TokenizerMapperTriplet
	extends Mapper<Object, Text, LongWritable[], LongWritable> {
		private LongWritable[] pair = new LongWritable()[2];
		private LongWritable username = new LongWritable();
		private LongWritable follower1 = new LongWritable();
		private LongWritable follower2 = new LongWritable();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				username.set(Long.parseLong(itr.nextToken()));
				if (itr.hasMoreTokens()) {
					follower1.set(Long.parseLong(itr.nextToken()));
					if (itr.hasMoreTokens()) {
						follower2.set(Long.parseLong(itr.nextToken()));
						//if (follower2.compareTo(follower1) > 0) {
						pair[0].set(follower1.get())
						pair[1].set(follower2.get())
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
	extends Reducer<LongWritable[],LongWritable,LongWritable,LongWritable> {
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
				for (int i = 0; i < allValues.size(); i++) {
					context.write(allValues.get(i),one);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(new Configuration(), SecondTriangle.class);
		conf.setJobName("13515029 second Triangle");
		conf.setJarByClass(SecondTriangle.class);
		FileSystem fs = FileSystem.get(conf);

		MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, TokenizerMapper.class);
		MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, TokenizerMapperTriplet.class);
		conf.setReducerClass(TriangleReducer.class);

		conf.setOutputKeyClass(LongWritable.class); 
		conf.setOutputValueClass(LongWritable.class);

		if (fs.exists(new Path(args[2]))) {
			fs.delete(new Path(args[2]), true);
		}

		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		System.exit(conf.waitForCompletion(true) ? 0 : 1);
	}
}