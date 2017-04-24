package org.cs6240;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.SerializationUtils;
import org.cs6240.utils.FileIOHelper;
import org.cs6240.utils.GeoUtils;
import org.cs6240.utils.HBaseHelper;
import org.json.JSONArray;
import org.json.JSONObject;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by dongxu on 4/16/17.
 */
public class KMeans {

    public static HashMap<String, String[]> cityTable;
    public static HashMap<String, ArrayList<double[]>> CityPoints;
    public static JSONArray allPointsJsonArray;
    private static Double ReservedWeight = 0.6;
    private static String SpecializedType = "3";
    private static String CurrentTable = "Venues";

    public static class Venue {
        public Double lat, lng, distanceToCenter;
        public String id;
        public Integer NumOfCheckIn;

        Venue(Double latitude, Double longitude, String venueId, Integer NumOfCheckIn){
            lat = latitude;
            lng = longitude;
            id = venueId;
            this.NumOfCheckIn = NumOfCheckIn;
        }
    }

    private static class HashablePoint{
        public Double lat,lng;
        HashablePoint(double a, double b){
            lat = a;
            lng = b;
        }

        @Override
        public int hashCode() {
            return ((Double)(lat * 1000000)).intValue() + ((Double)(lng * 1000000)).intValue();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            HashablePoint other = (HashablePoint) obj;
            return this.lat - other.lat < 0.000001 && this.lng - other.lat < 0.000001;
        }
    }


    private static class DoubleKeys implements WritableComparable {
        private String city;
        private HashablePoint point;
        private String lat, lng;
        public DoubleKeys() {}
        public DoubleKeys(String key, double[] coordinate){
            city = key;
            lat = ((Double)coordinate[0]).toString();
            lng = ((Double)coordinate[1]).toString();
        }

        @Override
        public String toString(){
            return (new StringBuilder()).append(city).append(',').append(lat).append(',').append(lng).toString();
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            city = WritableUtils.readString(in);
            lat = WritableUtils.readString(in);
            lng = WritableUtils.readString(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            WritableUtils.writeString(out, city);
            WritableUtils.writeString(out, lat);
            WritableUtils.writeString(out, lng);
        }

        @Override
        public int compareTo(Object o){
            DoubleKeys key = (DoubleKeys)o;
            int result = city.compareTo(key.city);
            if (0 == result){
                result = lat.compareTo(key.lat);
                if (0 == result){
                    result = lng.compareTo(key.lng);
                }
            }
            return result;
        }
    }

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

    private static Integer CalculateK(double density){
        return (int)density/15;
    }

    private static Double CalcDistance(double[] coordinate, double[] point){
        return GeoUtils.getDistance(coordinate[0], coordinate[1], point[0], point[1]);
    }

    private static Boolean CompareK(Set<double[]> last, Set<double[]> current){
        Set<HashablePoint> hashSet = new HashSet<>();

        for (double[] point: last) {
            hashSet.add(new HashablePoint(point[0], point[1]));
        }

        for (double[] point: current){
            if (!hashSet.contains(new HashablePoint(point[0], point[1])))
                return false;
        }
        return true;
    }

    private static HashMap<double[], ArrayList<Venue>> InitKPoints(
            HashMap<double[], ArrayList<Venue>> KPointsTable, ArrayList<double[]> coordinates, int k,
            Double lat, Double lng){

        double initFactorLength= 0.005 * k;
        for (int i=0; i<k; i++) {
            double[] coordinate;
            Double factor;
            do {
                factor = Math.random();
                factor -= 0.5;
                coordinate = new double[]{lat+factor*initFactorLength, lng+factor*initFactorLength};
            } while (KPointsTable.containsKey(coordinate));
            coordinates.add(coordinate);
            KPointsTable.put(coordinate, new ArrayList<>());
        }

        return KPointsTable;
    }

    private static HashMap<double[], List<Venue>> CalcRanges(HashMap<double[], ArrayList<Venue>> Points){
        HashMap<double[], List<Venue>> newHashMap = new HashMap<>();
        for (HashMap.Entry point: Points.entrySet()){
            ArrayList<Venue> updatedVenues = new ArrayList<>();
            for (Venue venue: (ArrayList<Venue>) point.getValue()) {
                venue.distanceToCenter = GeoUtils.getDistance(venue.lat, venue.lng, ((double[])point.getKey())[0], ((double[])point.getKey())[1]);
                updatedVenues.add(venue);
            }

            Collections.sort(updatedVenues, new Comparator<Venue>(){
                public int compare(Venue o1, Venue o2){
                    if (o1.distanceToCenter == o2.distanceToCenter)
                        return 0;
                    return o1.distanceToCenter < o2.distanceToCenter ? 1 : -1;
                }
            });

            if (updatedVenues.size() < 5)
                continue;
            List<Venue> filteredVenues = updatedVenues.subList(0, ((Double)((updatedVenues.size()-1)*(ReservedWeight))).intValue());
            if (filteredVenues.size() == 0)
                continue;
            Double maxDistance = filteredVenues.get(0).distanceToCenter;
            if (maxDistance < 1)
                continue;
            Double density = filteredVenues.size() / maxDistance;
            if (density < 0.0035)
                continue;

            double[] newKey = new double[]{((double[])point.getKey())[0], ((double[])point.getKey())[1], maxDistance, filteredVenues.size()};
            newHashMap.put(newKey, filteredVenues);
        }

        return newHashMap;
    }

    private static void BuildJsonForGoogleMap(HashMap<double[], List<Venue>> hashMap){

        for (double[] pointData: hashMap.keySet()) {
            JSONArray pointArray = new JSONArray();
            pointArray.put(pointData[0]);
            pointArray.put(pointData[1]);
            pointArray.put(pointData[2]);
            pointArray.put(pointData[3]);

            allPointsJsonArray.put(pointArray);


//            JSONObject obj = new JSONObject();
//            obj.put("type", "Feature");
//            JSONObject geometry = new JSONObject();
//            geometry.put("type", "Point");
//            geometry.put("coordinates", pointArray);
//
//            JSONObject properties = new JSONObject();
//            properties.put("mag", pointData[2]);
//            properties.put("density", pointData[3] / pointData[2]);
        }

    }

    private static String BuildJsonWithVenues(HashMap<double[], ArrayList<Venue>> hashMap, String City) {
        JSONArray allPoints = new JSONArray();
        for (HashMap.Entry entry : hashMap.entrySet()) {
            double[] point = (double[]) entry.getKey();
            ArrayList<Venue> venues = (ArrayList<Venue>) entry.getValue();

            JSONObject item = new JSONObject();
            JSONArray venuesJSON = new JSONArray();

            item.put("Lat", point[0]);
            item.put("Lng", point[1]);

            for (Venue venue : venues) {
                JSONArray venueJSON = new JSONArray();
                venueJSON.put(venue.id);
                venueJSON.put(venue.lat);
                venueJSON.put(venue.lng);
                venuesJSON.put(venueJSON);
            }

            item.put("Venues", venuesJSON);
            allPoints.put(item);
        }

        // Convert to JSON format
        JSONObject obj = new JSONObject();
        obj.put("City", City);
        obj.put("Points", allPoints);

        return obj.toString();
    }



    private static double[] CalcNewCenterPoint(ArrayList<Venue> venues){
        double[] point = {0.0, 0.0};
        int total = 0;
        for (Venue venue: venues){
            int currentWeight = venue.NumOfCheckIn;
            total += currentWeight;
            point[0] += venue.lat*currentWeight;
            point[1] += venue.lng*currentWeight;
        }
        point[0] /= total;
        point[1] /= total;

        point[0] = Math.round(point[0] * 1000000.0 ) / 1000000.0;
        point[1] = Math.round(point[1] * 1000000.0 ) / 1000000.0;

        return point;
    }

    public static void cityTableProcess(){
        cityTable = new HashMap<>();
        for (List<String> strings: FileIOHelper.DataFileReader.buffer){
            cityTable.put(strings.get(0), new String[]{strings.get(8), strings.get(1), strings.get(2)});
        }
    }

    public static class KMeansMapper
            extends Mapper<Object, Text, Text, Text> {

        HBaseHelper helper = null, backupHelper = null;

        @Override
        public void setup(Context context){
            try {
                helper = new HBaseHelper("Venues");
                backupHelper = new HBaseHelper("VenuesBackup");
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            JSONObject jsonObject = new JSONObject(value.toString());

            String city = jsonObject.getString("City");
            String type = jsonObject.getString("Cat");
            String id = jsonObject.getString("ID");
            JSONArray checkIn = jsonObject.getJSONArray("Check-Ins");

            assert (city != null && type != null);

            // For a certain type of venue only
            if (!type.equals(SpecializedType))
                return;

            HashMap<String, String> map = new HashMap<>();

            map.put("lat", jsonObject.getString("Lat"));
            map.put("lng", jsonObject.getString("Lng"));
            map.put("city", city);
            map.put("numOfCheckIn", ((Integer)checkIn.length()).toString());
            try {
                helper.addRecordFieldsByHashMap(id, "data", map);
                backupHelper.addRecordFieldsByHashMap(id, "data", map);
            } catch (Exception e){
                e.printStackTrace();
            }

            context.write(new Text(city), new Text(id));
        }
    }

    public static class KMeansSecondaryMapper
            extends TableMapper<DoubleKeys, Text> {

        HBaseHelper helper;

        @Override
        public void setup(Context context){
            try {
                helper = new HBaseHelper(CurrentTable);
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        public void map(ImmutableBytesWritable row, Result value, Context context
        ) throws IOException, InterruptedException {

            String latString = new String(value.getValue(Bytes.toBytes("data"), Bytes.toBytes("lat")));
            String lngString = new String(value.getValue(Bytes.toBytes("data"), Bytes.toBytes("lng")));
            String city = new String(value.getValue(Bytes.toBytes("data"), Bytes.toBytes("city")));

            ArrayList<Result> points = helper.getValueFilteredRecords(city, "City");

            double minDistance = Double.MAX_VALUE;
            double[] minPoint = null;
            double[] coordinate = new double[]{Double.parseDouble(latString), Double.parseDouble(lngString)};

            for (Result rs : points) {
                double[] point = (double[])SerializationUtils.deserialize(rs.getRow());
                double distance = CalcDistance(coordinate, point);
                if (distance < minDistance) {
                    minPoint = point;
                    minDistance = distance;
                }
            }

            DoubleKeys key = new DoubleKeys(city, minPoint);
            context.write(key, new Text(row.toString()));
        }
    }

    private static class KMeansPartitioner extends Partitioner<Text,NullWritable> {

        @Override
        public int getPartition(Text key, NullWritable value, int numReduceTasks) {
            if (numReduceTasks == 0)
                return 0;
            return key.hashCode() % numReduceTasks;
        }
    }

    private static class KMeansSecondaryPartitioner extends Partitioner<DoubleKeys,Text> {

        @Override
        public int getPartition(DoubleKeys key, Text value, int numReduceTasks) {
            if (numReduceTasks == 0)
                return 0;
            return key.city.hashCode() % numReduceTasks;
        }
    }

    public static class KMeansSecondaryGroupingComparator extends WritableComparator {
        public KMeansSecondaryGroupingComparator() {
            super(DoubleKeys.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b){
            DoubleKeys key1 = (DoubleKeys)a;
            DoubleKeys key2 = (DoubleKeys)b;

            // If the carrier of two rows are the same then they got grouped together
            return (key1.lat.compareTo(key2.lat) == 0 && key1.lng.compareTo(key2.lng) == 0)?0:1;
        }
    }

    public static class KMeansReducer
            extends Reducer<Text,Text,NullWritable,Text> {

        HBaseHelper helper = null;

        @Override
        public void setup(Context context) {
            try {
                helper = new HBaseHelper("Points");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), new Text(allPointsJsonArray.toString()));
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            String[] cityInfo = cityTable.get(key.toString());
            Double density = Double.parseDouble(cityInfo[0]);
            Double lat = Double.parseDouble(cityInfo[1]);
            Double lng = Double.parseDouble(cityInfo[2]);
            Integer k = CalculateK(density);
            HashMap<double[], ArrayList<Venue>> KPointsTable = new HashMap<>();
//            HashMap<double[], ArrayList<Venue>> NewKPointsTable = new HashMap<>();
//            HashMap<double[], ArrayList<Venue>> LastPointsTable = null;
            ArrayList<double[]> coordinates = new ArrayList<>();
//            ArrayList<double[]> newPoints = new ArrayList<>();
//            ArrayList<Venue> venueList = new ArrayList<>();

            // Generate initial K points
            InitKPoints(KPointsTable, coordinates, k, lat, lng);

            try {
                for (double[] p : coordinates)
                    helper.addRecordSingleField(p, "data", "City", key.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class KMeansSecondaryReducer
            extends Reducer<DoubleKeys,Text,NullWritable,Text> {

        HBaseHelper helper = null;
        HashMap<String, ArrayList<double[]>> newPointsList = null;

        @Override
        public void setup(Context context) {
            newPointsList = new HashMap<>();
            try {
                helper = new HBaseHelper("Venues");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            HBaseHelper pointHelper = new HBaseHelper("Points");
            for (Map.Entry e : newPointsList.entrySet()) {
                String city = (String) e.getKey();
                ArrayList<Result> oldPoints = pointHelper.getValueFilteredRecords(city, "City");
                HashSet<HashablePoint> oldPointsHashSet = new HashSet<>();
                int matchedPoints = 0;
                int previousPoints = oldPoints.size();

                for (Result rs : oldPoints) {
                    double[] point = (double[]) SerializationUtils.deserialize(rs.getRow());
                    oldPointsHashSet.add(new HashablePoint(point[0], point[1]));
                }

                for (double[] p : (ArrayList<double[]>) e.getValue()) {
                    if (oldPointsHashSet.contains(new HashablePoint(p[0], p[1]))) {
                        matchedPoints++;
                    }
                }

                // Should not end at this round
                if (matchedPoints != previousPoints) {
                    try {
                        for (HashablePoint entry : oldPointsHashSet) {
                            pointHelper.removeRow(new double[]{entry.lat, entry.lng});
                        }
                        for (double[] p : (ArrayList<double[]>) e.getValue())
                            pointHelper.addRecordSingleField(p, "data", "City", city);
                    } catch (Exception err) {
                        err.printStackTrace();
                    }
                } else {
                    // remove related venues so that the total size of venues will shrink
                    // and thus the points of current city will be constant
                    ArrayList<Result> results = helper.getValueFilteredRecords(city, "city");
                    for (Result rs : results) {
                        helper.removeRow(rs.getRow());
                    }
                }
            }
        }

        public void reduce(DoubleKeys key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            String city = key.city;
            ArrayList<Venue> venueList = new ArrayList<>();

            // Get all venues from HBase
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                Text currentValue = iterator.next();
                Result rs = helper.getOneRecord(currentValue.toString());
                Double latitude = Double.parseDouble(Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("lat"))));
                Double longitude = Double.parseDouble(Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("lng"))));
                String id = Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("id")));
                Integer checkInNumber = Integer.parseInt(Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("numOfCheckIn"))));
                venueList.add(new Venue(latitude, longitude, id, checkInNumber));
            }

            double[] newPoint = CalcNewCenterPoint(venueList);
            if (!newPointsList.containsKey(city))
                newPointsList.put(city, new ArrayList<>());
            newPointsList.get(city).add(newPoint);
        }
    }

        public static class KMeansThirdReducer
                extends Reducer<DoubleKeys,Text,NullWritable,Text> {

            HBaseHelper helper = null;
            HashMap<double[], ArrayList<Venue>> PointsMap = null;

            @Override
            public void setup(Context context){
                PointsMap = new HashMap<>();
                try {
                    helper = new HBaseHelper("VenuesBackup");
                } catch (Exception e){
                    e.printStackTrace();
                }
            }

            @Override
            public void cleanup(Context context) throws IOException, InterruptedException{
                for (Map.Entry e: PointsMap.entrySet()){
                    //String city = (String)e.getKey();
                    BuildJsonForGoogleMap(CalcRanges((PointsMap)));
                }
                context.write(NullWritable.get(), new Text(allPointsJsonArray.toString()));
            }

            public void reduce(DoubleKeys key, Iterable<Text> values,
                               Context context
            ) throws IOException, InterruptedException {

                Double lat = Double.parseDouble(key.lat);
                Double lng = Double.parseDouble(key.lng);
                ArrayList<Venue> venueList = new ArrayList<>();

                // Get all venues from HBase
                Iterator<Text> iterator = values.iterator();
                while (iterator.hasNext()) {
                    Text currentValue = iterator.next();
                    Result rs = helper.getOneRecord(currentValue.toString());
                    Double latitude = Double.parseDouble(Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("lat"))));
                    Double longitude = Double.parseDouble(Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("lng"))));
                    String id = Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("id")));
                    Integer checkInNumber = Integer.parseInt(Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("numOfCheckIn"))));

                    venueList.add(new Venue(latitude, longitude, id, checkInNumber));
                }
                PointsMap.put(new double[]{lat, lng}, venueList);
            }
        }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();

        if (otherArgs.length < 3) {
            System.err.println("Usage: KMeans <Venue json file> " +
                    "<City file> <Output file>");
            System.exit(2);
        }

//        List<String[]> lines = FileIOHelper.CSVFileReader.load(otherArgs[2]);
//        categoryTableProcess(lines);

        // Hbase
        HBaseHelper.createTable("Venues", "data");
        HBaseHelper.createTable("VenuesBackup", "data");
        HBaseHelper.createTable("Points", "data");

        FileIOHelper.DataFileReader.open(args[1]);
        cityTableProcess();
        //KMeans.CityPoints = new HashMap<>();
        KMeans.allPointsJsonArray = new JSONArray();
        Job job1 = Job.getInstance(conf1, "KMeans");

        job1.setJarByClass(KMeans.class);
        job1.setPartitionerClass(KMeansPartitioner.class);
        job1.setMapperClass(KMeansMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(KMeansReducer.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

        if (!job1.waitForCompletion(true))
            System.exit(1);


        HBaseHelper helper = new HBaseHelper("Venues");
        Integer MaxRound = 200, round = 0;
        while (round < MaxRound && helper.rowCount() > 0){
            round++;
            Configuration conf = new HBaseConfiguration();
            Scan scan = new Scan();
            scan.setCaching(500);
            scan.setCacheBlocks(false);

            Job Iteratejob = Job.getInstance(conf, "Iterating");

            TableMapReduceUtil.initTableMapperJob(
                    "Venues",
                    scan,
                    KMeansSecondaryMapper.class,
                    DoubleKeys.class,
                    Text.class,
                    Iteratejob);

            Iteratejob.setJarByClass(KMeans.class);
            Iteratejob.setPartitionerClass(KMeansSecondaryPartitioner.class);

            Iteratejob.setGroupingComparatorClass(KMeansSecondaryGroupingComparator.class);
            Iteratejob.setReducerClass(KMeansSecondaryReducer.class);

            if (!Iteratejob.waitForCompletion(true))
                System.exit(1);
        }

        CurrentTable = "VenuesBackup";

        Configuration conf = new HBaseConfiguration();
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        Job Lastjob = Job.getInstance(conf, "Output");

        TableMapReduceUtil.initTableMapperJob(
                "VenuesBackup",
                scan,
                KMeansSecondaryMapper.class,
                DoubleKeys.class,
                Text.class,
                Lastjob);

        Lastjob.setJarByClass(KMeans.class);
        Lastjob.setMapperClass(KMeansSecondaryMapper.class);
        Lastjob.setPartitionerClass(KMeansSecondaryPartitioner.class);

        Lastjob.setGroupingComparatorClass(KMeansSecondaryGroupingComparator.class);
        Lastjob.setReducerClass(KMeansThirdReducer.class);
        Lastjob.setOutputKeyClass(Text.class);
        Lastjob.setOutputValueClass(NullWritable.class);

        if (!Lastjob.waitForCompletion(true))
            System.exit(1);

        System.exit(0);

    }

}