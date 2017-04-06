package org.cs6240.utils;

/**
 * Created by dongxu on 4/5/17.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;


public class CityColumnJoin {

    public static class CityColumnMapper
            extends Mapper<Object, Text, Text, Text>{

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = FileIOHelper.TabLineParse(value.toString());

            Double minDistance = Double.MAX_VALUE;
            List<String> nearest = null;

            for (List<String> city: FileIOHelper.CityFileReader.buffer){
                Double cityYCoordination = Double.parseDouble(city.get(1));
                Double cityXCoordination = Double.parseDouble(city.get(2));
                Double venueYCoordination = Double.parseDouble(fields[1]);
                Double venueXCoordination = Double.parseDouble(fields[2]);

                Double distance = GeoUtils.getDistance(
                        cityYCoordination, cityXCoordination,
                        venueYCoordination, venueXCoordination);

                if (distance < minDistance){
                    minDistance = distance;
                    nearest = city;
                }
            }

            assert(nearest != null);
            ArrayList<String> newLine = new ArrayList<>(Arrays.asList((String[])fields));
            newLine.add(nearest.get(0));
            newLine.add(nearest.get(nearest.size()-1));

            String newLineString = "";
            for (String field: newLine){
                newLineString += "\t" + field;
            }
            newLineString = newLineString.substring(1);

            //FileIOHelper.TabLineBuilder((String[])newLine.toArray());
            //Text result = new Text();
            context.write(new Text(), new Text(newLineString));
        }
    }

//    private static class CityColumnPartitioner extends Partitioner<Text,Text> {
//
//        @Override
//        public int getPartition(Text key, Text value, int numReduceTasks) {
//            return 0;
//        }
//    }

    public static class CityColumnReducer
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
            System.err.println("Usage: CityColumnJoin <City file> <Venue file> <New venue file>");
            System.exit(2);
        }

        FileIOHelper.CityFileReader.open(args[0]);

        Job job1 = Job.getInstance(conf1, "CityColumnJoin");
        //job1.setNumReduceTasks(1);
        //job1.setPartitionerClass(CityColumnPartitioner.class);
        job1.setJarByClass(CityColumnJoin.class);
        job1.setMapperClass(CityColumnMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(CityColumnReducer.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

        if (!job1.waitForCompletion(true))
            System.exit(1);

        System.exit(0);

    }
}