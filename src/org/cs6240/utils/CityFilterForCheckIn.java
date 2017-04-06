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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

/**
 * Created by dongxu on 4/6/17.
 */
public class CityFilterForCheckIn {

    public static HashMap<String, String> set;

    public static void PreProcess(){
        set = new HashMap<>();
        for (List<String> strings:FileIOHelper.DataFileReader.buffer){
            set.put(strings.get(0), strings.get(5));
        }
    }

    public static class CheckInMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = FileIOHelper.TabLineParse(value.toString());

            if (set.get(fields[1]) != null){
                ArrayList<String> newLine = new ArrayList<>(Arrays.asList((String[])fields));
                newLine.add(set.get(fields[1]));
                newLine.add(fields[1]);

                String newLineString = "";
                for (String field: newLine){
                    newLineString += "\t" + field;
                }
                newLineString = newLineString.substring(1);
                context.write(new Text(), new Text(newLineString));
            }
        }
    }

//    private static class CityColumnPartitioner extends Partitioner<Text,Text> {
//
//        @Override
//        public int getPartition(Text key, Text value, int numReduceTasks) {
//            return 0;
//        }
//    }

    public static class CheckInReducer
            extends Reducer<Text,Text,NullWritable,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                context.write(NullWritable.get(), iterator.next());
            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();

        if (otherArgs.length < 3) {
            System.err.println("Usage: CityFilterForCheckIn <Venue file> <Check-in file> <New Check-in file>");
            System.exit(2);
        }

        FileIOHelper.DataFileReader.open(args[0]);
        PreProcess();

        Job job1 = Job.getInstance(conf1, "CityColumnJoin");

        job1.setJarByClass(CityColumnJoin.class);
        job1.setMapperClass(CheckInMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(CheckInReducer.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

        if (!job1.waitForCompletion(true))
            System.exit(1);

        System.exit(0);

    }

}
