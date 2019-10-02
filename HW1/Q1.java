import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 


public class Q1 {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	      
	    	String row = value.toString();
			String[] cols = row.split("\t");
			if(cols.length != 2) return;
	        
            String user = cols[0];
            String[] friends = cols[1].split(",");
            String frnd1;
            String frnd2;
            for (int i = 0; i < friends.length; i++) {
                frnd1 = friends[i];
                context.write(new Text(user), new Text("1," + frnd1)); 
                for (int j = i+1; j < friends.length; j++) {
                    frnd2 = friends[j];
                    context.write(new Text(frnd1), new Text("2," + frnd2)); 
                    context.write(new Text(frnd2), new Text("2," + frnd1));   
                }
            }
	    }
	} 

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        
	    	String[] value;
	        HashMap<String, Integer> recommendedFrnds = new HashMap<String, Integer>();
	        for (Text val : values) {
	            value = (val.toString()).split(",");
	            if (value[0].equals("1")) {
	                recommendedFrnds.put(value[1], -1);
	            } else if (value[0].equals("2")) {
	                if (recommendedFrnds.containsKey(value[1])) {
	                    if (recommendedFrnds.get(value[1]) != -1) {
	                        recommendedFrnds.put(value[1], recommendedFrnds.get(value[1]) + 1);
	                    }
	                } else {
	                    recommendedFrnds.put(value[1], 1);
	                }
	            }
	        }
	        ArrayList<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>();
	        for (Entry<String, Integer> entry : recommendedFrnds.entrySet()) {
	            if (entry.getValue() != -1) {   
	                list.add(entry);
	            }
	        }
	        Collections.sort(list, new Comparator<Entry<String, Integer>>() {
	            public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {
	                int val = e2.getValue().compareTo(e1.getValue());
	                if(val == 0) return e1.getKey().compareTo(e2.getKey());
	                else return val;
	            }
	        });
	        
	        
	        
            ArrayList<String> top = new ArrayList<String>();
            for (int i = 0; i < Math.min(10, list.size()); i++) {
                top.add(list.get(i).getKey());
            }
            StringBuilder reco = new StringBuilder();

            for(int i = 0; i < Math.min(10,list.size()); i++){
            	if(i == 0) reco.append(list.get(i).getKey());
            	else reco.append("," + list.get(i).getKey());
            }

            context.write(key, new Text(reco.toString()));
	        
	    }
	}
	
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

			if (otherArgs.length != 2) {
				System.err.println("Usage: <in> <out>");
				System.exit(1);
			}
			 
	        Job job = Job.getInstance(conf, "Recommend Friends");
	        job.setJarByClass(Q1.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	 
	        job.setMapperClass(Map.class);
	        job.setReducerClass(Reduce.class);
	 
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	 
	        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	 
	        job.waitForCompletion(true);
		}
}
