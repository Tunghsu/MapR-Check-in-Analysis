package org.cs6240;

import com.sun.org.apache.xpath.internal.operations.Bool;
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
    public static JSONArray allPointsJsonArray;

    public static class Venue {
        public Double lat, lng, distanceToCenter;
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
        return (int)density/15;
    }

    private static Double CalcDistance(double[] coordinate, Venue venue){
        return GeoUtils.getDistance(coordinate[0], coordinate[1], venue.lat, venue.lng);
    }

    private static class HashablePoint{
        Double lat,lng;
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
            List<Venue> filteredVenues = updatedVenues.subList(0, ((Double)((updatedVenues.size()-1)*(0.))).intValue());
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
        for (Venue venue: venues){
            point[0] += venue.lat;
            point[1] += venue.lng;
        }
        point[0] /= venues.size();
        point[1] /= venues.size();

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

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            JSONObject jsonObject = new JSONObject(value.toString());

            String city = jsonObject.getString("City");
            String type = jsonObject.getString("Cat");

            assert(city != null && type != null);

            // For a certain type of venue only
            if (!type.equals("3"))
                return;

            //System.out.println(key);
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

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            //context.write(NullWritable.get(), new Text(allPointsJsonArray.toString()));
        }
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
                    if (CompareK(LastPointsTable.keySet(), NewKPointsTable.keySet())) {
                        //System.out.println("Final");
                        break;
                    }
                }
                LastPointsTable = NewKPointsTable;
                coordinates = newPoints;
                newPoints = new ArrayList<>();
                KPointsTable = new HashMap<>();
                NewKPointsTable = new HashMap<>();
                //System.out.println(i);
            }

            HashMap<double[], List<Venue>> FinalKPointsTable = CalcRanges(LastPointsTable);

            //BuildJsonForGoogleMap(FinalKPointsTable);

            String jsonTextWithVenue = BuildJsonWithVenues(LastPointsTable, key.toString());

            context.write(NullWritable.get(), new Text(jsonTextWithVenue));

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
        KMeans.allPointsJsonArray = new JSONArray();
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