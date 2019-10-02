import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class Q3 {
	
//	static String user1, user2;
//	static Job job2Pointer;
//	static HashMap<String, String> mutualFriends = new HashMap<>();
	
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
			user1  = context.getConfiguration().get("user1");
			user2  = context.getConfiguration().get("user2");
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			int count = 0;
			
			for(Text t : values){
				count++;
			}

			if(count > 1){
				context.write(new Text(user1 + "," + user2), key);
			}
			
		}
		
	}
	public static class ReplicatedJoinMapper extends
	Mapper<Object, Text, Text, Text> {
		
		static HashSet<String> mutual = new HashSet<>();
		
		//Distributed cache does not work on the cluster, pls use this code for the setup phase instead.
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

			Configuration conf = context.getConfiguration();
			String myfilepath = conf.get("firstJobOutputLocation");
			Path part=new Path("hdfs://cshadoop1" + myfilepath); //Location of file in HDFS
//			part = new Path(myfilepath);
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			
			for (FileStatus status : fss) {
				
				Path pt = status.getPath();
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				
				while ((line=br.readLine()) != null){
					
					String[] cols = line.split("\t");
					if(cols.length != 2) return;
					
					String frnd = cols[1];
					mutual.add(frnd);
				}
			}
		}
	
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
	
			String row = value.toString();
			String[] cols = row.split(",");
			if(cols.length != 10) return;
			
			String user = cols[0];
			String nameZip = cols[1] + " " + cols[2] + ":" + cols[6];
			
			if(mutual.contains(user)) {
				context.write(new Text(""), new Text(nameZip));
			}
			
		}
		
	}
	
	public static class MutualFriendsInfoReducer extends
	Reducer<Text, Text, Text, Text> {
		
		static String user1, user2;
		
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			user1 = context.getConfiguration().get("user1");
			user2 = context.getConfiguration().get("user2");
		
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuilder mutualFriends = new StringBuilder("");
			
			int count = 0;
			
			for(Text t : values){
				if(count++ == 0) mutualFriends.append(t);
				else mutualFriends.append(", " + t);
			}
			
			context.write(null, new Text(user1 + ", " + user2 + ", [" + mutualFriends.toString() + "]"));
			
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
		if (otherArgs.length != 5) {
			System.err.println("Usage: <in1> <in2> <out> <user1> <user2>");
			System.exit(1);
		}
		
		String user1 = otherArgs[3];
		String user2 = otherArgs[4];
	
		Job job = Job.getInstance(conf, "List Mutual Friends"); 
		job.setJarByClass(Q3.class); 
		
		job.setMapperClass(FriendMapper.class);
		job.setReducerClass(CommonFriendReducer.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2] + "_temp"));
	    
	    job.getConfiguration().set("user1", user1);
	    job.getConfiguration().set("user2", user2);

	    if(job.waitForCompletion(true)==true){
	    	
	    	Job job2 = Job.getInstance(conf, "List Joined Mutual Friends & Zip"); 
	    	job2.setJarByClass(Q3.class); 
	    
	    	job2.setMapperClass(ReplicatedJoinMapper.class);
	    	job2.setReducerClass(MutualFriendsInfoReducer.class);
	
	    	job2.setOutputKeyClass(Text.class);
	    	job2.setOutputValueClass(Text.class);
	    	
	    	FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
	        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
	        
	        job2.getConfiguration().set("firstJobOutputLocation", otherArgs[2] + "_temp");
		    job2.getConfiguration().set("user1", user1);
		    job2.getConfiguration().set("user2", user2);

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
	        
	    }
	
	}
	
}
