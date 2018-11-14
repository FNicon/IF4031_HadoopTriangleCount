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
	/*public static void equal(String name1, String name2) {
		return (name1.compareTo(name2) == 0);
	}
	public static void greater_than(String name1, String name2) {
		return (name1.compareTo(name2) > 0);
	}
	public static void less_than(String name1, String name2) {
		return (name1.compareTo(name2) < 0);
	}
	public static void read_file(String filename) {
		try {
			fr = new FileReader("file.txt");
			br = new BufferedReader(fr);
			String line;
			String username;
			String follower;
			while ((line = br.readLine()) != null) {
				String[] lineDetail = line.split("\\t",2);
				lineDetail[0]
				// process the line
				System.out.println(line);
			}
		} catch (FileNotFoundException e) {
			System.err.println("Can not find specified file!");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Can not read from file!");
			e.printStackTrace();
		} finally {
			if (br != null) try { br.close(); } catch (IOException e) { /* ensure close */ //}
			//if (fr != null) try { fr.close(); } catch (IOException e) { /* ensure close */ }
		//}
	//}
	/*public static class Relationship {
		private String username;
		private String follower;

		public Relationship(String inputUsername, String inputFollower) {
			username = inputUsername;
			follower = inputFollower;
		}
	}*/
	public static class TokenizerMapper
	extends Mapper<Object, Text, IntWritable, IntWritable> {
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

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			alreadyProcessed.size();
			StringTokenizer itr = new StringTokenizer(value.toString());
			int count = 0;
			while (itr.hasMoreTokens()) {
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
			}
		}
	}

	/*public static class Pair extends ArrayWritable {
		public Pair(IntWritable[] values) {
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
		private IntWritable username = new IntWritable();
		//private Pair[] triplets

		public void reduce(IntWritable key, Iterable<IntWritable> values,
		Context context
		) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				if (count == 0) {
					pair[0].set(val.get());
				} else {
					pair[1].set(val.get());
					context.write(key, pair);
					count = 0;
				}
				count = count + 1;
				//sum += val.get();
			}
			//result.set(sum);
			//context.write(key, result);
		}
	}

	public static class SwapMapper
	extends Mapper<IntWritable, IntWritable[], IntWritable[], IntWritable> {
		private IntWritable[] pair = new IntWritable()[2];
		private IntWritable tripletMaster = new IntWritable();

		public void map(IntWritable key, Iterable<IntWritable> value, Context context
		) throws IOException, InterruptedException {
			context.write(value, key);
		}
	}

	public static class SwapMapperQuestion
	extends Mapper<IntWritable, IntWritable, IntWritable[], IntWritable> {
		private IntWritable[] pair = new IntWritable()[2];
		private IntWritable question = new IntWritable(-1);

		public void map(IntWritable key, IntWritable value, Context context
		) throws IOException, InterruptedException {
			pair[0] = key;
			pair[1] = value;
			context.write(pair, question);
		}
	}

	public static class TripletReducer
	extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable[]> {
		private IntWritable[] pair = new IntWritable()[2];
		private IntWritable username = new IntWritable();
		//private Pair[] triplets

		public void reduce(IntWritable key, Iterable<IntWritable> values,
		Context context
		) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				if (count == 0) {
					pair[0].set(val.get());
				} else {
					pair[1].set(val.get());
					context.write(key, pair);
					count = 0;
				}
				count = count + 1;
				//sum += val.get();
			}
			//result.set(sum);
			//context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
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