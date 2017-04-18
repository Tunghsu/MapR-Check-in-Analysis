package org.cs6240;

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
import org.cs6240.utils.CityColumnJoin;
import org.cs6240.utils.FileIOHelper;
import org.cs6240.utils.GeoUtils;
import org.json.JSONArray;
import org.json.JSONObject;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by dongxu on 4/16/17.
 */
public class KMeans {

    public static HashMap<String, String[]> cityTable;
    public static HashMap<String, ArrayList<double[]>> CityPoints;

    public static class Venue {
        public Double lat, lng;
        public String id;

        Venue(Double latitude, Double longitude, String venueId){
            lat = latitude;
            lng = longitude;
            id = venueId;
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
        return (int)density/50;
    }

    private static Double CalcDistance(double[] coordinate, Venue venue){
        return GeoUtils.getDistance(coordinate[0], coordinate[1], venue.lat, venue.lng);
    }

    private static double[] CalcNewCenterPoint(ArrayList<Venue> venues){
        double[] point = {0.0, 0.0};
        for (Venue venue: venues){
            point[0] += venue.lat;
            point[1] += venue.lng;
        }
        point[0] /= venues.size();
        point[1] /= venues.size();

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

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            JSONObject jsonObject = new JSONObject(value.toString());

            String city = jsonObject.getString("City");
            String type = jsonObject.getString("Cat");

            // For a certain type of venue only
            if (!type.equals("3"))
                return;

            context.write(new Text(city), value);
        }
    }

    private static class KMeansPartitioner extends Partitioner<Text,Text> {

        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            if (numReduceTasks == 0)
                return 0;
            return key.hashCode() % numReduceTasks;
        }
    }

    public static class KMeansReducer
            extends Reducer<Text,Text,NullWritable,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            String[] cityInfo = cityTable.get(key.toString());
            Double density = Double.parseDouble(cityInfo[0]);
            Double lat = Double.parseDouble(cityInfo[1]);
            Double lng = Double.parseDouble(cityInfo[2]);
            Integer k = CalculateK(density);
            double initFactorLength= 0.005 * k;
            HashMap<double[], ArrayList<Venue>> KPointsTable = new HashMap<>();
            HashMap<double[], ArrayList<Venue>> NewKPointsTable = new HashMap<>();
            HashMap<double[], ArrayList<Venue>> LastPointsTable = null;
            ArrayList<double[]> coordinates = new ArrayList<>();
            ArrayList<double[]> newPoints = new ArrayList<>();
            ArrayList<Venue> venueList = new ArrayList<>();

            // Generate initial K points
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

            // Get All venues
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                Text currentValue = iterator.next();
                JSONObject jsonObject = new JSONObject(currentValue.toString());
                Double latitude = Double.parseDouble(jsonObject.getString("Lat"));
                Double longitude = Double.parseDouble(jsonObject.getString("Lng"));
                String id = jsonObject.getString("ID");
                venueList.add(new Venue(latitude, longitude, id));
            }


            Integer Times = 100;
            for (int i=0; i<Times; i++) {

                for (Venue currentVenue: venueList){
                    double minDistance = Double.MAX_VALUE;
                    double[] minPoint = null;

                    for (double[] coordinate : coordinates) {
                        double distance = CalcDistance(coordinate, currentVenue);
                        if (distance < minDistance) {
                            minPoint = coordinate;
                            minDistance = distance;
                        }
                    }

                    if (!KPointsTable.containsKey(minPoint)){
                        KPointsTable.put(minPoint, new ArrayList<>());
                    }

                    KPointsTable.get(minPoint).add(currentVenue);
                }

                for (HashMap.Entry entry : KPointsTable.entrySet()) {
                    ArrayList<Venue> venues = (ArrayList<Venue>) entry.getValue();
                    double[] newPoint = CalcNewCenterPoint(venues);
                    newPoints.add(newPoint);
                    NewKPointsTable.put(newPoint, venues);
                }

                if (LastPointsTable != null){
                    if (LastPointsTable.keySet().equals(NewKPointsTable.keySet()))
                        break;
                }
                LastPointsTable = NewKPointsTable;
                coordinates = newPoints;
                newPoints = new ArrayList<>();
                KPointsTable = new HashMap<>();
                NewKPointsTable = new HashMap<>();
            }

            JSONArray allPoints = new JSONArray();
            for (HashMap.Entry entry: LastPointsTable.entrySet()){
                double[] point = (double[])entry.getKey();
                ArrayList<Venue> venues = (ArrayList<Venue>) entry.getValue();

                JSONObject item = new JSONObject();
                JSONArray venuesJSON = new JSONArray();

                item.put("Lat", point[0]);
                item.put("Lng", point[1]);

                for (Venue venue: venues){
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
            obj.put("City", key.toString());
            obj.put("Points", allPoints);

            context.write(NullWritable.get(), new Text(obj.toString()));

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

        FileIOHelper.DataFileReader.open(args[1]);
        cityTableProcess();
        //KMeans.CityPoints = new HashMap<>();

        Job job1 = Job.getInstance(conf1, "KMeans");

        job1.setJarByClass(CityColumnJoin.class);
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

        System.exit(0);

    }

}