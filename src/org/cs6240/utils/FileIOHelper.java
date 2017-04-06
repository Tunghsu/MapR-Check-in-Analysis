package org.cs6240.utils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.opencsv.CSVReader;


/**
 * Created by dongxu on 4/6/17.
 */
public class FileIOHelper {

    public static class DataFileReader {
        static BufferedReader reader;
        static List<List<String>> buffer;
        Integer index;

        DataFileReader(){
            index  = 0;
        }

        public static void open(String DataFilePath) throws IOException{
            BufferedReader reader;
            reader = new BufferedReader(new FileReader(DataFilePath));
            buffer = new ArrayList<>();

            String line;
            while ((line=reader.readLine()) != null){
                String[] fields= line.split("\t");
                List<String> row = Arrays.asList(fields);
                buffer.add(row);
            }

        }

        public static void reset() throws IOException{
            reader.reset();
        }

        public boolean hasNext(){
                return this.index < buffer.size();
        }

        public List<String> next(){
            return buffer.get(index++);
        }
    }

    public static class DataFileWriter {

        public static void write(String DataFilePath, List<List<String>> lines) throws IOException{
            Path file = Paths.get(DataFilePath);
            List<String> output = new ArrayList<>();
            for (List<String> row:lines){
                String line = "";
                for (String field: row){
                    line += "\t" + field;
                }
                output.add(line.substring(1));
            }
            Files.write(file, output, Charset.forName("UTF-8"));
        }
    }


    public static class CityInfoReader {

        public static List<String[]> load(String CityInfoFilePath) throws IOException{
            CSVReader reader = new CSVReader(new FileReader(CityInfoFilePath));
            List<String[]> lines = reader.readAll();

            return lines;

        }
    }

    public static String[] TabLineParse(String line){
        return line.split("\t");
    }

    public static synchronized String TabLineBuilder(String[] row){
        String line = "";
        for (String field: row){
            line += "\t" + field;
        }
        return line.substring(1);
    }
}
