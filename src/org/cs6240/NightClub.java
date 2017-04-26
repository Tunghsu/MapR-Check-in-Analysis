package org.cs6240;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.cs6240.utils.CityColumnJoin;
import org.cs6240.utils.FileIOHelper;
import org.cs6240.utils.HBaseHelper;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by dongxu on 4/24/17.
 */
public class NightClub {

    //public static HashMap<String, String> cityTable, categoryTable;


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

    private static String checkDayInWeek(String timestamp){
        Date date = new Date(Long.parseLong(timestamp));
        DateFormat format2=new SimpleDateFormat("EEE");

        String finalDay=format2.format(date);

        return finalDay;
    }

    private static class DoubleKeys implements WritableComparable {
        public String country, userid;
        public DoubleKeys() {}
        public DoubleKeys(String country, String id){
            this.country = country;
            this.userid = id;
        }

        @Override
        public String toString(){
            return (new StringBuilder()).append(country).append(',').append(userid).toString();
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            country = WritableUtils.readString(in);
            userid = WritableUtils.readString(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            WritableUtils.writeString(out, country);
            WritableUtils.writeString(out, userid);
        }

        @Override
        public int compareTo(Object o){
            DoubleKeys key = (DoubleKeys)o;
            int result = country.compareTo(key.country);
            if (0 == result){
                result = userid.compareTo(key.userid);
            }
            return result;
        }
    }

    public static class NightClubMapper
            extends Mapper<Object, Text, DoubleKeys, DerivedArrayWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            JSONObject jsonObject = new JSONObject(value.toString());

            String city = jsonObject.getString("City");
            String country = jsonObject.getString("Country");
            String type = jsonObject.getString("Cat");
            String id = jsonObject.getString("ID");
            JSONArray checkIn = jsonObject.getJSONArray("Check-Ins");
            String subCat = jsonObject.getString("Sub_Cat");
            Boolean isFoo = subCat.contains("Bar") || type.equals("5");
            Boolean isBar = type.equals("6");

            if (!isFoo && !isBar)
                return;
            for (int i=0; i<checkIn.length(); i++) {
                JSONArray item = checkIn.getJSONArray(i);
                String userId = item.getString(0);
                String timestamp = item.getString(1);

                String day = checkDayInWeek(timestamp);
                if (isFoo && (day.equals("Mon") || day.equals("Tue") || day.equals("Wed") || day.equals("Thu"))){
                    context.write(new DoubleKeys(country, userId), new DerivedArrayWritable(new String[]{timestamp, "1"}));
                    return;
                }
                if (isBar && (day.equals("Sun") || day.equals("Sat"))){
                    context.write(new DoubleKeys(country, userId), new DerivedArrayWritable(new String[]{timestamp, "2"}));
                }
            }
        }
    }

    private static class NightClubPartitioner extends Partitioner<DoubleKeys,DerivedArrayWritable> {

        @Override
        public int getPartition(DoubleKeys key, DerivedArrayWritable value, int numReduceTasks) {
            if (numReduceTasks == 0)
                return 0;
            return (key.country.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static class NightClubGroupingComparator extends WritableComparator {
        public NightClubGroupingComparator() {
            super(DoubleKeys.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b){
            DoubleKeys key1 = (DoubleKeys)a;
            DoubleKeys key2 = (DoubleKeys)b;

            // If the carrier of two rows are the same then they got grouped together
            return (key1.country.compareTo(key2.country) == 0 && key1.userid.compareTo(key2.userid) == 0)?0:1;
        }
    }

    public static class NightClubReducer
            extends Reducer<DoubleKeys,DerivedArrayWritable,Text,Text> {

        private String key = null;
        private HashMap<String, Integer> total = new HashMap<>();
        private HashMap<String, Integer> qualified = new HashMap<>();

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
//
//            try {
//                HBaseHelper.createTable("ClubTable", "data");
//                HBaseHelper helper = new HBaseHelper("ClubTable");
//
//                HashMap<String, String> header = new HashMap<>();
//                header.put("total", totalCheckin.toString());
//                header.put("qualified", qualifiedCheckin.toString());
//                helper.addRecordFieldsByHashMap(key, "data", header);
//            } catch (Exception e){
//                e.printStackTrace();
//            }
            for (String country : qualified.keySet()) {
                context.write(new Text(country), new Text(total.get(country).toString()
                        +" "+qualified.get(country)+" "
                        +((Double)(((double)(qualified.get(country)))/((double)(total.get(country))))).toString()));
            }
        }

        public void reduce(DoubleKeys key, Iterable<DerivedArrayWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            Integer checkin = 0;
            Integer qualifiedCheckin = 0;
            if (!total.containsKey(key.country))
                total.put(key.country, 0);
            if (!qualified.containsKey(key.country))
                qualified.put(key.country, 0);

            HashSet<String> clubTable = new HashSet<>();
            HashSet<String> professionTable = new HashSet<>();
            Iterator<DerivedArrayWritable> iterator = values.iterator();
            SimpleDateFormat weekOfYear = new SimpleDateFormat("w");
            while (iterator.hasNext()) {
                DerivedArrayWritable currentValue = iterator.next();
                String[] rowValues = currentValue.toStrings();
                Date checkinDate = new Date(Long.parseLong(rowValues[0]));
                String week = weekOfYear.format(checkinDate);
                if (rowValues[1].equals("1")) {
                    if (!clubTable.contains(week)) {
                        clubTable.add(week);
                        checkin++;
                    }
                } else {
                    professionTable.add(week);
                }
            }

            for (String week: clubTable){
                if (professionTable.contains(week)){
                    qualifiedCheckin++;
                }
            }

            total.put(key.country, total.get(key.country)+checkin);
            qualified.put(key.country, qualified.get(key.country)+qualifiedCheckin);
        }
    }

    public static class NightClubSecondaryMapper
            extends Mapper<Object, Text, Text, NullWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            context.write(value, NullWritable.get());
        }
    }

    public static class NightClubSecondaryReducer
            extends Reducer<Text,NullWritable,Text,Text> {

        private Integer totalCheckin = 0;
        private Integer qualifiedCheckin = 0;

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            context.write(new Text(totalCheckin.toString()), new Text(qualifiedCheckin.toString()));
        }

        public void reduce(Text key, Iterable<NullWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

//            HashSet<String> clubTable = new HashSet<>();
//            HashSet<String> professionTable = new HashSet<>();
//            Iterator<DerivedArrayWritable> iterator = values.iterator();
//            SimpleDateFormat weekOfYear = new SimpleDateFormat("w");
//            while (iterator.hasNext()) {
//                DerivedArrayWritable currentValue = iterator.next();
//                String[] rowValues = currentValue.toStrings();
//                Date checkinDate = new Date(Long.parseLong(rowValues[0]));
//                String week = weekOfYear.format(checkinDate);
//                if (rowValues[1].equals("1")) {
//                    clubTable.add(week);
//                    totalCheckin++;
//                } else {
//                    professionTable.add(week);
//                }
//            }
//
//            for (String week: clubTable){
//                if (professionTable.contains(week)){
//                    qualifiedCheckin++;
//                }
//            }
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: NightClubStats <Aggregated Venue file> <Output file>");
            System.exit(2);
        }

//        List<String[]> lines = FileIOHelper.CSVFileReader.load(otherArgs[2]);
//        categoryTableProcess(lines);

//        FileIOHelper.DataFileReader.open(args[3]);
//        cityTableProcess();

        Job job1 = Job.getInstance(conf1, "NightClubStats");

        job1.setJarByClass(NightClub.class);
        job1.setPartitionerClass(NightClubPartitioner.class);
        job1.setMapperClass(NightClubMapper.class);
        job1.setMapOutputKeyClass(DoubleKeys.class);
        job1.setMapOutputValueClass(DerivedArrayWritable.class);
        job1.setGroupingComparatorClass(NightClubGroupingComparator.class);

        job1.setNumReduceTasks(1);
        job1.setReducerClass(NightClubReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

        if (!job1.waitForCompletion(true))
            System.exit(1);



        System.exit(0);

    }

}