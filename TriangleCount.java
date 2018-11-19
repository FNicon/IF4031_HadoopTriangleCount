import java.io.IOException;
import java.util.StringTokenizer;
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

public class TriangleCount {
	public static class TokenizerMapper
	extends Mapper<Text, Text, LongWritable, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private LongWritable countValue = new LongWritable();
		private LongWritable username = new LongWritable();

		public void map(Text key, Text value, Context context
		) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				username.set(Long.parseLong(itr.nextToken()));
				if (itr.hasMoreTokens()) {
					countValue.set(Long.parseLong(itr.nextToken()));
					context.write(username, countValue);
				}
			}
		}
	}

	public static class IntSumReducer
	extends Reducer<LongWritable,LongWritable,Text,Text> {
		private LongWritable result = new LongWritable();

		public void reduce(LongWritable key, Iterable<LongWritable> values,
		Context context
		) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			long resultKey = key.get();
			long resultVal = result.get();
			context.write(Text(resultKey.toString()), Text(resultVal.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "13515029 triangle count");
		job.setJarByClass(TriangleCount.class);
		
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}