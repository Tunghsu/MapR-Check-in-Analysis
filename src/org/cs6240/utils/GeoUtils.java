package org.cs6240.utils;

/**
 * Created by dongxu on 4/6/17.
 */
public class GeoUtils {

    // From http://stackoverflow.com/questions/3715521/how-can-i-calculate-the-distance-between-two-gps-points-in-java
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

    // granularity = 100, distance = 8.2m
    public static String getGridKey(String city, Double lat, Double lng, Integer granularity){
        lat *= 1000000;
        lng *= 1000000;
        Integer lat_int = lat.intValue();
        Integer lng_int = lng.intValue();

        // might be hash conflicts
        String hashKey = city + ((Integer)(lat_int/granularity)).toString() +
                ((Integer)(lng_int/granularity)).toString();

        return hashKey;
    }
}
