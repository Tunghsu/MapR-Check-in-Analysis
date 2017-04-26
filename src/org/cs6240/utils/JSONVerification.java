package org.cs6240.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.codehaus.jettison.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

/**
 * Created by dongxu on 4/24/17.
 */
public class JSONVerification {

    public static class JSONVerificationMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            try{
                JSONObject jsonObject = new JSONObject(value.toString());
                jsonObject.put("x", "y");
            } catch (org.json.JSONException e){
                e.printStackTrace();
                System.out.println(value.toString());
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();

        if (otherArgs.length < 1) {
            System.err.println("Usage: JSONVerification <Venue file>");
            System.exit(2);
        }


        Job job1 = Job.getInstance(conf1, "JSONVerification");

        job1.setJarByClass(CityColumnJoin.class);
        job1.setMapperClass(JSONVerificationMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        job1.setOutputFormatClass(NullOutputFormat.class);

        if (!job1.waitForCompletion(true))
            System.exit(1);

        System.exit(0);

    }
}
