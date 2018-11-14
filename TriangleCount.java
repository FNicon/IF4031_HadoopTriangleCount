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

public class TriangleCount {
	public static class TokenizerMapper
	extends Mapper<IntWritable[], IntWritable, IntWritable, IntWritable> {
		private IntWritable username = new IntWritable();
		private IntWritable follower = new IntWritable();
		private ArrayList<Integer> alreadyProcessed = new ArrayList<Integer>();

		private boolean isNotAlreadyProcessed(IntWritable follower) {
			int i = 0;
			while ((i + 1 < alreadyProcessed.size()) && (alreadyProcessed.get(i).compareTo(follower) != 0)) {
				i = i + 1
			}
			return (alreadyProcessed.get(i).compareTo(follower) != 0);
		}

		public void map(Iterable<IntWritable> keys, IntWritable value, Context context
		) throws IOException, InterruptedException {
			alreadyProcessed.size();
			//StringTokenizer itr = new StringTokenizer(value.toString());
			//int count = 0;
			for (IntWritable key : keys) {
				if (follower.compareTo(username) > 0) {
					context.write(username, follower);
				}
			}
			/*while (itr.hasMoreTokens()) {
				if (count == 0) {
					username.set(itr.nextToken());
				} else if (count == 1) {
					follower.set(itr.nextToken());
				}
				if (count == 1) {
					count = 0;
					if (follower.compareTo(username) > 0) {
						context.write(username, follower);
						//alreadyProcessed should be pair of username, follower
						alreadyProcessed.add(username);
					} else {
						if (isNotAlreadyProcessed(follower)) {
							context.write(follower, username);
						}
					}
				} else {
					count = count + 1;
				}
			}*/
		}
	}

	/*public static class IntArrayWritable extends ArrayWritable {
		public IntArrayWritable(IntWritable[] values) {
			super(IntWritable.class, values);
		}

		@Override
		public IntWritable[] get() {
			return (IntWritable[]) super.get();
		}
	}*/

	public static class TripletReducer
	extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable[]> {
		private IntWritable[] pair = new IntWritable()[2];
		private IntWritable empty = new IntWritable(-1);
		private List<IntWritable> allConnected = new ArrayList<>();

		public void reduce(IntWritable key, Iterable<IntWritable> values,
		Context context
		) throws IOException, InterruptedException {
			for (IntWritable val : values) {
				allConnected.add(val);
			}
			if (allConnected.size() > 1) {
				for (int i = 0; i < allConnected.size(); i++) {
					for (int j = 0; j < allConnected.size(); j++) {
						pair[0].set(allConnected.get(i));
						pair[1].set(allConnected.get(j));
						context.write(key, pair);
					}
				}
			} else if (allConnected.size() == 1) {
				pair[0].set(allConnected.get(0));
				pair[1].set(key);
				context.write(empty, pair);
			}
			/*int count = 0;
			for (IntWritable val : values) {
				for (IntWritable checkedVal : values) {
					if (checkedVal.compareTo(val) != 0) {
						pair[0].set(val.get());
						pair[1].set(checkedVal.get());
						context.write(key, pair);
					}
				}
			}*/
		}
	}

	public static class SwapMapper
	extends Mapper<IntWritable, IntWritable[], IntWritable[], IntWritable> {
		private IntWritable empty = new IntWritable(-1);

		public void map(IntWritable key, Iterable<IntWritable> values, Context context
		) throws IOException, InterruptedException {
			for (IntWritable val : values) {
				if (empty.compareTo(key) == 0) {
					context.write(values, empty);
				} else {
					context.write(values, key);
				}
			}
		}
	}

	public static class TriangleReducer
	extends Reducer<IntWritable[],IntWritable,IntWritable,IntWritable> {
		private IntWritable one = new IntWritable(1);
		private IntWritable empty = new IntWritable(-1);

		public void reduce(Iterable<IntWritable> keys, IntWritable value,
		Context context
		) throws IOException, InterruptedException {
			if (empty.compareTo(value)) {
				for (IntWritable key : keys) {
					context.write(key, one);
				}
			}
		}
	}

	public static class NothingMap
	extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values,
		Context context
		) throws IOException, InterruptedException {
			context.write(key, values);
		}
	}

	public static class IntSumReducer
	extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values,
		Context context
		) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "triangle count");
		job.setJarByClass(TriangleCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(TripletReducer.class);
		job.setReducerClass(TripletReducer.class);

		job.setMapperClass(SwapMapper.class);
		job.setCombinerClass(TriangleReducer.class);
		job.setReducerClass(TriangleReducer.class);

		job.setMapperClass(NothingMap.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}