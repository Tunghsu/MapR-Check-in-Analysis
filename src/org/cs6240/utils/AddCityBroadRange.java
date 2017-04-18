package org.cs6240.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by dongxu on 4/14/17.
 */
public class AddCityBroadRange {

    public static ConcurrentHashMap<String, List<String>> cityTable;


    public static class DerivedArrayWritable extends ArrayWritable {
        public DerivedArrayWritable() {
            super(Text.class);
        }

        public DerivedArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    public static void cityTableProcess(){
        cityTable = new ConcurrentHashMap<>();
        for (List<String> strings: FileIOHelper.DataFileReader.buffer){
            cityTable.put(strings.get(0), strings);
        }
    }

    public static class AddCityBroadRangeMapper
            extends Mapper<Object, Text, Text, DerivedArrayWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = FileIOHelper.TabLineParse(value.toString());
            DerivedArrayWritable coordinates;
            coordinates = new DerivedArrayWritable(fields);
            context.write(new Text(fields[6]), coordinates);
        }
    }

    public static class AddCityBroadRangeReducer
            extends Reducer<Text,DerivedArrayWritable,NullWritable,Text> {

        public void reduce(Text key, Iterable<DerivedArrayWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            List<String> cityInfo = cityTable.get(key.toString());
            Double lat, lng;
            lat = Double.parseDouble(cityInfo.get(1));
            lng = Double.parseDouble(cityInfo.get(2));
            Double minLat = lat, maxLat = lat, minLng = lng, maxLng = lng;

            Iterator<DerivedArrayWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                DerivedArrayWritable currentValue = iterator.next();
                String[] rowValues = currentValue.toStrings();
                Double currentLat, currentLng;
                currentLat = Double.parseDouble(rowValues[1]);
                currentLng = Double.parseDouble(rowValues[2]);

                if (minLat > currentLat){
                    minLat = currentLat;
                }
                if (maxLat < currentLat){
                    maxLat = currentLat;
                }
                if (minLng > currentLng){
                    minLng = currentLng;
                }
                if (maxLng < currentLng){
                    maxLng = currentLng;
                }
            }

            Double distance = getDistance(minLat, minLng, maxLat, maxLng) / 1000;

            Double density = (((double) Integer.parseInt(cityInfo.get(6))) / (distance*distance/2));

            String newLineString = "";
            for (String field: cityInfo){
                newLineString += "\t" + field;
            }

            newLineString += "\t" + distance.toString();
            newLineString += "\t" + density.toString();
            newLineString = newLineString.substring(1);

            context.write(NullWritable.get(), new Text(newLineString));

        }

        public static synchronized double getDistance(double lat_a, double lng_a, double lat_b, double lng_b) {
            double pk = 180/3.14159;

            double a1 = lat_a / pk;
            double a2 = lng_a / pk;
            double b1 = lat_b / pk;
            double b2 = lng_b / pk;

            double t1 = Math.cos(a1)*Math.cos(a2)*Math.cos(b1)*Math.cos(b2);
            double t2 = Math.cos(a1)*Math.sin(a2)*Math.cos(b1)*Math.sin(b2);
            double t3 = Math.sin(a1)*Math.sin(b1);
            double tt = Math.acos(t1 + t2 + t3);

            return 6366000*tt;
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();

        if (otherArgs.length < 3) {
            System.err.println("Usage: AddCityBroadRange <Venue file> " +
                    "<City file> <Output file>");
            System.exit(2);
        }

//        List<String[]> lines = FileIOHelper.CSVFileReader.load(otherArgs[2]);
//        categoryTableProcess(lines);

        FileIOHelper.DataFileReader.open(args[1]);
        cityTableProcess();

        Job job1 = Job.getInstance(conf1, "CityColumnJoin");

        job1.setJarByClass(CityColumnJoin.class);
        job1.setMapperClass(AddCityBroadRangeMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DerivedArrayWritable.class);

        job1.setReducerClass(AddCityBroadRangeReducer.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

        if (!job1.waitForCompletion(true))
            System.exit(1);

        System.exit(0);

    }

}