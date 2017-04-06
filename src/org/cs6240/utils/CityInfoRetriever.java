package org.cs6240.utils;

/**
 * Created by dongxu on 4/6/17.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


class CityStructure{
    CityStructure(String country, String population, String year){
        this.year = year;
        this.population = population;
        this.country = country;
    }
    String country;
    String population;
    String year;
}

public class CityInfoRetriever {

    private static HashMap<String, CityStructure> map;

    private static void loadCityInfo(String CityInfoFilePath) throws IOException{
        List<String[]> rows = FileIOHelper.CityInfoReader.load(CityInfoFilePath);
        map = new HashMap<>();

        for (String[] fields: rows){
            CityStructure cityInfo = map.get(fields[4]);
            if ((cityInfo == null) || (Integer.parseInt(cityInfo.year) > Integer.parseInt(fields[8]))){
                map.put(fields[4].toLowerCase(), new CityStructure(fields[0], fields[9], fields[8]));
            }
        }
    }

    private static void loadCities(String CityFilePath) throws IOException {
        FileIOHelper.DataFileReader.open(CityFilePath);
    }

    private static List<List<String>> join(){
        FileIOHelper.DataFileReader reader = new FileIOHelper.DataFileReader();
        CityStructure cityInfo;
        List<List<String>> output = new ArrayList<>();
        List<String> row;
        while(reader.hasNext()){
            List<String> fields = reader.next();
            if ((cityInfo = map.get(fields.get(0).toLowerCase())) != null){
                row = new ArrayList<>(Arrays.asList((String[])fields.toArray()));
                //, cityInfo.country, cityInfo.population, cityInfo.year));
                row.add(cityInfo.country);
                row.add(cityInfo.population);
                row.add(cityInfo.year);
                output.add(row);
            } else {
                System.out.println(fields.toString());
            }
        }

        return output;

    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3){
            System.out.println("Usage: program <CityInfoFile> <CityFile> <New CityFile>");
        }

        loadCityInfo(args[0]);
        loadCities(args[1]);

        List<List<String>> result = join();
        FileIOHelper.DataFileWriter.write(args[2], result);
    }
}
