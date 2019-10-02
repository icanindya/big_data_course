
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q2 {

	public static class FriendMapper
	extends Mapper<Object, Text, Text, Text>{
		
		static String user1, user2;
		
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			user1  = context.getConfiguration().get("user1");
			user2  = context.getConfiguration().get("user2");
		}
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				
			String row = value.toString();
			String[] cols = row.split("\t");
			if(cols.length != 2) return;
			
			String user = cols[0];
			String[] frnds = cols[1].split(",");
			
			if(!(user.equals(user1) || user.equals(user2))) return;
			
			for (String frnd : frnds) {
				if(frnd.equals(user1) || frnd.equals(user2)) continue;
				context.write(new Text(frnd), new Text(user));
			}
			
		}
	}
	
	

	public static class CommonFriendReducer
	extends Reducer<Text, Text, Text, Text> {
		
		static String user1, user2;
		
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			user1 = context.getConfiguration().get("user1");
			user2 = context.getConfiguration().get("user2");
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			int count = 0;
			
			for(Text t : values){
				count++;
			}

			if(count > 1){
				context.write(null, new Text(key.toString()));
			}
			
		}
		
	}
	
	public static class DummyMapper
	extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if(context != null)
				context.write(new Text(""), value);
		}
	}

	public static class DummyReducer
		extends Reducer<Text, Text, Text, Text> {
		
		static String user1, user2;
		
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			user1 = context.getConfiguration().get("user1");
			user2 = context.getConfiguration().get("user2");
		
		}
			
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			StringBuilder mutualFriends = new StringBuilder();
			
			int count = 0;
			
			for(Text t : values){
				if(count++ == 0) mutualFriends.append(t);
				else mutualFriends.append(", " + t);
			}
			
			context.write(new Text(user1 + ", " + user2), new Text(mutualFriends.toString()));

		}
	}
		

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 4) {
			System.err.println("Usage: <in> <out> <user1> <user2>");
			System.exit(1);
		}
		
		String user1 = otherArgs[2];
		String user2 = otherArgs[3];

		Job job = Job.getInstance(conf, "List Mutual Friends"); 
		job.setJarByClass(Q2.class); 
		
		job.setMapperClass(FriendMapper.class);
		job.setReducerClass(CommonFriendReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "_temp"));
        
        job.getConfiguration().set("user1", user1);
        job.getConfiguration().set("user2", user2);
        
        
        if(job.waitForCompletion(true) == true){
 
        	Job job2 = Job.getInstance(conf, "Print Mutual Friends"); 
        	job2.setJarByClass(Q2.class);
        	
        	job2.setMapperClass(DummyMapper.class);
        	job2.setReducerClass(DummyReducer.class);
        	
        	job2.setOutputKeyClass(Text.class);
        	job2.setOutputValueClass(Text.class);
        	
        	FileInputFormat.addInputPath(job2, new Path(otherArgs[1] + "_temp"));
        	FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
        	
        	job2.getConfiguration().set("user1", user1);
            job2.getConfiguration().set("user2", user2);
        	
        	System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
        
	}
}