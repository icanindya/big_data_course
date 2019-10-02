import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class Q4 {
	
	
	public static class FriendsMapper extends Mapper<Object, Text, Text, Text>{
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String row = value.toString();
			String[] cols = row.split("\t");
			if(cols.length != 2) return;
			
			String user = cols[0];
			
			String[] frnds = cols[1].split(",");
			for(int i = 0; i < frnds.length; i++){
				String frnd = frnds[i];
				context.write(new Text(frnd), new Text("A" + user));
			}
		}
	}
	
	public static class AgeMapper extends Mapper<Object, Text, Text, Text>{
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String row = value.toString();
			String[] cols = row.split(",");
			if(cols.length != 10) return;
			
			String user = cols[0];
			Integer age = Util.getAge(cols[9]);
			
			context.write(new Text(user), new Text("B" + age));
		}
	}
	
	public static class FriendAgeReducer extends Reducer<Text, Text, Text, Text>{
		
		private List<String> users = new ArrayList<>();
		static String frnd, frndAge;
		
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			frnd = key.toString();
			users = new ArrayList<String>();
			
			
			for(Text t : values){
				if(t.charAt(0) == 'A') users.add(t.toString().substring(1));
				else if(t.charAt(0) == 'B') frndAge = frnd + "," + t.toString().substring(1);
			}
			
			for(String user : users){
				context.write(new Text(user), new Text(frndAge));
			}
			
		}
		
	}
	
	
	public static class AvgAgeMapper extends Mapper<Object, Text, Text, Text>{
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String row = value.toString();
			String[] cols = row.split("\t");
			if(cols.length != 2) return;
			
			String user = cols[0];
			String age = cols[1].split(",")[1];
			
			context.write(new Text(user), new Text(age));
			
		}
	}
	
	public static class AvgAgeReduer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String user = key.toString();
			
			int sum = 0;
			int count = 0;
			for(Text t : values){
				sum += Integer.parseInt(t.toString());
				count++;
			}
			
			String avgFrndAge = (int)Math.ceil(sum/count) + "";
			
			context.write(new Text(user), new Text(avgFrndAge));
			
		}
	}
	
	public static class UserAvgFrndAge implements Comparable<UserAvgFrndAge>{
		String user;
		Integer avgFrndAge;
		String address;
		
		public UserAvgFrndAge(String user, Integer avgFrndAge){
			this.user = user;
			this.avgFrndAge = avgFrndAge;
		}
		
		public UserAvgFrndAge(String user, Integer avgFrndAge, String address){
			this.user = user;
			this.avgFrndAge = avgFrndAge;
			this.address = address;
		}

		@Override
		public int compareTo(UserAvgFrndAge o) {
			return o.avgFrndAge - this.avgFrndAge;
		}

	}
	
	public static class SortAvgAgeMapper extends Mapper<Object, Text, Text, Text>{
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String row = value.toString();
			String[] cols = row.split("\t");
			if(cols.length != 2) return;
			
			String user = cols[0];
			Integer avgFrndAge = Integer.parseInt(cols[1]); 
			
			context.write(new Text(""), new Text(user + "," + avgFrndAge));
		}
	}
	
	public static class SortAvgAgeReduer extends Reducer<Text, Text, Text, Text>{
		
		List<UserAvgFrndAge> list = new ArrayList<>();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				
			for(Text t : values){
				String[] cols = t.toString().split(",");
				String user = cols[0];
				Integer avgFrndAge = Integer.parseInt(cols[1]); 
				
				list.add(new UserAvgFrndAge(user, avgFrndAge));
			}
			
			Collections.sort(list);
			
			for(UserAvgFrndAge o : list){
				context.write(new Text(o.user), new Text(o.avgFrndAge.toString()));
			}
			
		}
	}
	
	
	
	public static class TopUserAddressMapper extends Mapper<Object, Text, Text, Text>{
		static LinkedHashMap<String, String> topUsers = new LinkedHashMap<>();
		
		//Distributed cache does not work on the cluster, pls use this code for the setup phase instead.
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

			Configuration conf = context.getConfiguration();
			String myfilepath = conf.get("userdataLocation");

			Path part=new Path("hdfs://cshadoop1" + myfilepath); //Location of file in HDFS
//			part = new Path(myfilepath);
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			
			for (FileStatus status : fss) {
				
				Path pt = status.getPath();
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				int count = 0;
				
				while ((line=br.readLine()) != null && ++count !=21){
					
					String[] cols = line.split("\t");
					if(cols.length != 2) return;
					
					String user = cols[0];
					String avgFrndAge = cols[1];
					topUsers.put(user, avgFrndAge);
				}
			}
		}
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				
			String row = value.toString();
			String[] cols = row.split(",");
			if(cols.length != 10) return;
			
			String user = cols[0];
			
			if(topUsers.containsKey(user)){
				String address = cols[3] + ", " + cols[4] + ", " + cols[5];
				String info = user + "," + topUsers.get(user) + "," + address;
				context.write(new Text(""), new Text(info));
			}
		}
	}
	
	
	public static class TopUserFrndAgeReduer extends Reducer<Text, Text, Text, Text>{
		
		static List<UserAvgFrndAge> list = new ArrayList<>();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				
			for(Text t : values){
				String[] info = t.toString().split(",");
				String user = info[0];
				Integer avgFrndAge = Integer.parseInt(info[1]);
				String address = t.toString().substring(t.toString().indexOf(",", t.toString().indexOf(",") + 1) + 1);
				list.add(new UserAvgFrndAge(user, avgFrndAge, address));
			}
			
			Collections.sort(list);
			
			for(UserAvgFrndAge o : list){
				context.write(null, new Text(o.user + ", " + o.address + ", " + o.avgFrndAge));
			}
			
		}
	}
	
	
	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
		if (otherArgs.length != 5) {
			System.err.println("Usage: <in1> <in2> <out1> <out2> <out3>");
			System.exit(1);
		}
	
		Job job = Job.getInstance(conf, "Friend Age Aggregator");
		job.setJarByClass(Q4.class); 
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, FriendsMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, AgeMapper.class);
		
		job.setReducerClass(FriendAgeReducer.class);
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2] + "_temp"));
	
		if(job.waitForCompletion(true)==true){
			Job job2 = Job.getInstance(conf, "Average Friend Age");
			job2.setJarByClass(Q4.class);
			
			job2.setMapperClass(AvgAgeMapper.class);
			job2.setReducerClass(AvgAgeReduer.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job2, new Path(otherArgs[2] + "_temp"));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
			
			if(job2.waitForCompletion(true) == true){
				Job job3 = Job.getInstance(conf, "Sort by Average Friend Age");
				job3.setJarByClass(Q4.class);
				
				job3.setMapperClass(SortAvgAgeMapper.class);
				job3.setReducerClass(SortAvgAgeReduer.class);
				
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);
				
				FileInputFormat.addInputPath(job3, new Path(otherArgs[2]));
				FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));
				
				if(job3.waitForCompletion(true) == true){
					Job job4 = Job.getInstance(conf, "Print Address of Top Users");
					job4.setJarByClass(Q4.class);
					
					job4.setMapperClass(TopUserAddressMapper.class);
					job4.setReducerClass(TopUserFrndAgeReduer.class);
					
					job4.setOutputKeyClass(Text.class);
					job4.setOutputValueClass(Text.class);
					
					FileInputFormat.addInputPath(job4, new Path(otherArgs[1]));
					FileOutputFormat.setOutputPath(job4, new Path(otherArgs[4]));
				
					job4.getConfiguration().set("userdataLocation", otherArgs[3]);
					
					System.exit(job4.waitForCompletion(true) ? 0 : 1);

				}
				
			}		
		
		}
	
	}
	
}
