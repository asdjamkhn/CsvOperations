package com.example.CsvOperations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CsvReader {
    public List<Mapping> readMapFile(String filepath) throws IOException, CsvValidationException {
        List<Mapping>mappingList=new ArrayList<>();
        try(Reader reader=new FileReader(filepath)){
            CSVReader csvReader=new CSVReader(reader);
            String[] nextRecord;
            while((nextRecord= csvReader.readNext())!=null){
                Mapping mapping=new Mapping();
                mapping.setSourceHeader(nextRecord[0]);
                mapping.setTargetHeader(nextRecord[1]);
                mapping.setDataType(nextRecord[2]);
                mapping.setMetadata(nextRecord[3]);
                mapping.setCompare(nextRecord[4]);
                mappingList.add(mapping);
            }
        }
        return mappingList;
    }

    public void printMappingList(List<Mapping>mappingList){
         for(Mapping mapping:mappingList){
            System.out.println("SourceHeader: " + mapping.getSourceHeader());
            System.out.println("TargerHeader: " + mapping.getTargetHeader());
            System.out.println("DataType: " + mapping.getDataType());
            System.out.println("MetaData: " + mapping.getMetadata());
            System.out.println("Compare: " + mapping.getCompare());
        }
    }

    public List<String[]> readCsvFile(String filePath1) throws IOException{
        // Open the first CSV file using a BufferedReader
        BufferedReader file1 = null;

        file1 = new BufferedReader(new FileReader(filePath1));

        // Create a list to store the data from the first CSV file
        List<String[]> data1 = new ArrayList<>();

        // Read the data from the first CSV file and store it in the data1 list
        String csv_line;
        while ((csv_line = file1.readLine()) != null) {
            data1.add(csv_line.split(","));
        }
        return data1;
    }
    public void csvFilesComparison(List<Mapping> mappingList, List <String[]> data1, List <String[]> data2) throws ParseException {

        int sourceHeader_counter=0;
        int targetHeader_counter=0;
        int metadatacounter=0;
        int compareColCounter=0;
        int data1Counter=0;
        int data2Counter=0;

        String[] keys1 = data1.get(0);
        LinkedHashMap<String,Integer> data1_map=new LinkedHashMap<>();
        for(int i =0;i<keys1.length;i++) {
            data1_map.put(keys1[i],data1Counter);
            data1Counter++;
        }
        System.out.println("\nData1-Counter K-V Pair is \n" + data1_map);

        String[] keys2 = data2.get(0);
        LinkedHashMap<String,Integer> data2_map=new LinkedHashMap<>();
        for(int i =0;i<keys2.length;i++) {
            data2_map.put(keys2[i],data2Counter);
            data2Counter++;
        }
        System.out.println("\nData1-Counter K-V Pair is \n" + data2_map);


        LinkedHashMap<Integer, String> compare_map = new LinkedHashMap<Integer,String>();
        for(int i=1; i<mappingList.size();i++){
            Mapping mapping = mappingList.get(i);
            compare_map.put(compareColCounter,mapping.getCompare());
            compareColCounter++;
        }

        System.out.println("\nCounter-Compare K-V Pair is \n" + compare_map);

        LinkedHashMap<Integer,String> metadata_map=new LinkedHashMap<Integer, String>();
        for(int i =1; i<mappingList.size();i++){
            Mapping mapping=mappingList.get(i);
            metadata_map.put(metadatacounter, mapping.getMetadata());
            metadatacounter++;
        }
        System.out.println("\nCounter-Metadata K-V Pair is \n" + metadata_map);

        LinkedHashMap<String,Integer> sourceCounter_map=new LinkedHashMap<String, Integer>();

        for(int i=1;i< mappingList.size();i++){
            Mapping mapping=mappingList.get(i);
            sourceCounter_map.put(mapping.getSourceHeader(),sourceHeader_counter);
            sourceHeader_counter++;
        }
        System.out.println("\nSourceHeader-Counter K-V Pair is \n"+sourceCounter_map);

        LinkedHashMap<String,Integer> targetCounter_map=new LinkedHashMap<String,Integer>();

        for(int i=1;i<mappingList.size();i++){
            Mapping mapping=mappingList.get(i);
            targetCounter_map.put(mapping.getTargetHeader(),targetHeader_counter);
            targetHeader_counter++;
        }
        System.out.println("\nTargetHeader-Counter K-V Pair is \n"+ targetCounter_map);

        //int map_row_count=1;
        int csv_file_count=0;
        LinkedHashMap<String, String> sourceHeader_map = new LinkedHashMap<String, String>();
        for(int i=1;i<mappingList.size();i++){
            Mapping mapping=mappingList.get(i);
            sourceHeader_map.put(mapping.getSourceHeader(),mapping.getDataType());
            csv_file_count++;
        }
        System.out.println("\nSourceHeader-DataType K-V Pair is \n"+ sourceHeader_map);

        LinkedHashMap<String,String> targetHeader_map=new LinkedHashMap<String,String>();
        for(int i=1;i<mappingList.size();i++){
            Mapping mapping = mappingList.get(i);
            targetHeader_map.put(mapping.getTargetHeader(),mapping.getDataType());
            csv_file_count++;
        }

        System.out.println("\nTargetHeader-DataType K-V Pair is \n"+ targetHeader_map);

        Set<Map.Entry<String, String>> entry = sourceHeader_map.entrySet();
        Iterator<Map.Entry<String, String>> iterator = entry.iterator();



        ExecutorService executorService = Executors.newFixedThreadPool(10);

        System.out.println("\n\n----------------------------------------Comparison----------------------------------------\n");

        int columnIndex =0;int value=0;
        String metadata_value="";
        String compare_value="";
        //String
        int n1=0;int n2=0; String item="";
        //int number=0;
        BigDecimal bd1=null; BigDecimal bd2=null;
        while(iterator.hasNext()) {

            Map.Entry<String, String> firstPair = iterator.next();

            if (firstPair.getValue().equals("Date")) {

                value = Integer.parseInt(String.valueOf(sourceCounter_map.get(data1.get(0)[columnIndex])));
                metadata_value = String.valueOf(metadata_map.get((value)));
                compare_value = String.valueOf(compare_map.get(value));




                if (!compare_value.equals("no")) {
//                    System.out.println("\nI am Date");
                    for (int i = 1; i < data1.size(); i++) {
                        SimpleDateFormat sdf = new SimpleDateFormat(metadata_value);
                        Date dob1 = sdf.parse(data1.get(i)[value]);
                        Date dob2 = sdf.parse(data2.get(i)[value]);

                        if (!dob1.equals(dob2)) {
                            System.out.println("Row: " + i + " \t\t\tDatatype: " + firstPair.getValue() + " \t\t\tMismatched Values: " + data1.get(i)[value] + "!= " + data2.get(i)[value]);
                        }
                    }
                }
            } else if (firstPair.getValue().equals("Integer")) {

                value = Integer.parseInt(String.valueOf(sourceCounter_map.get(data1.get(0)[columnIndex])));
                compare_value=String.valueOf(compare_map.get(value));

            if(!compare_value.equals("no")){
//                    System.out.println("\nI am Integer");
                    for (int i = 1; i < data1.size(); i++) {
                        n1 = Integer.parseInt(data1.get(i)[value]);
                        n2 = Integer.parseInt(data2.get(i)[value]);
                        if (n1 != n2) {
                            System.out.println("Row: " + i + " \t\t\tDatatype: " + firstPair.getValue() + " \t\t\tMismatched Values: " + data1.get(i)[value] + "!= " + data2.get(i)[value]);
                        }
                    }
                }
            } else if (firstPair.getValue().equals("String")) {
//
//                if(!sourceCounter_map.containsKey(data1_map.get(data1.get(0)[columnIndex]))){
//
//                    System.out.println("I am " + data1_map.get(data1.get(0)[columnIndex]));
//                    System.out.println("I am " +sourceCounter_map.get(data1.get(1)[2]));
//                    columnIndex++;
//                }
//
                value = Integer.parseInt(String.valueOf(sourceCounter_map.get(data1.get(0)[columnIndex])));

                compare_value=String.valueOf(compare_map.get(value));
//                System.out.println("I am compare value " + compare_value);
                if(!compare_value.equals("no")) {
//                    System.out.println("\nI am String");
                    for (int i = 1; i < data1.size(); i++) {
                        if (!(data1.get(i)[value].equals(data2.get(i)[value]))) {
                            System.out.println("Row: " + i + " \t\t\tDatatype: " + firstPair.getValue() + " \t\t\tMismatched Values: " + data1.get(i)[value] + "!= " + data2.get(i)[value]);
                        }
                    }
                }
            } else if (firstPair.getValue().equals("BigDecimal")) {

//                if(!sourceCounter_map.containsKey(data1_map.get(data1.get(0)[columnIndex]))){
//                    System.out.println("I am " + data1_map.get(data1.get(0)[columnIndex]));
//                    columnIndex++;
//                }
//                number = data1_map.get(data1.get(0)[columnIndex]);

                value = Integer.parseInt(String.valueOf(sourceCounter_map.get(data1.get(0)[columnIndex])));
                metadata_value= String.valueOf(metadata_map.get(value));
                compare_value=String.valueOf(compare_map.get(sourceCounter_map.get(data1.get(0)[columnIndex])));

                if(!compare_value.equals("no")) {
//                    System.out.println("\nI am Bigdecimal");
                    for (int i = 1; i < data1.size(); i++) {
                        bd1 = new BigDecimal(data1.get(i)[value]);
                        bd1 = bd1.setScale(Integer.parseInt(metadata_value), RoundingMode.HALF_UP);
                        bd2 = new BigDecimal(data2.get(i)[value]);
                        bd2 = bd2.setScale(Integer.parseInt(metadata_value), RoundingMode.HALF_UP);
                        if (bd1.compareTo(bd2) != 0) {
                            System.out.println("Row: " + i + " \t\t\tDatatype: " + firstPair.getValue() + " \t\t\tMismatched Values: " + data1.get(i)[value] + "!= " + data2.get(i)[value]);
                        }
                    }
                }
            }
            columnIndex++;
        }
    }
    public void jsonComparison(String filePath1, String filePath2) throws FileNotFoundException, JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();
        JsonParser jsonParser=new JsonParser();

        //Read file1
        FileReader reader1=new FileReader(filePath1);
        JsonObject obj1 = (JsonObject) jsonParser.parse(reader1);

        //Readfile2
        FileReader reader2=new FileReader(filePath2);
        JsonObject obj2 = (JsonObject) jsonParser.parse(reader2);

        Gson g=new Gson();
        //Getting a map object which contains String as key and Object as value
        Type mapType = new TypeToken<Map<String,Object>>(){}.getType();

        Map<String, Object> firstMap = g.fromJson(obj1,mapType);
        Map<String, Object> secondMap = g.fromJson(obj2,mapType);

        MapDifference<String, Object> difference = Maps.difference(firstMap,secondMap);

        //Comparing files
        System.out.println("\n\nComparing Data from file 1:");
        if (mapper.readTree(String.valueOf(obj1)).equals(mapper.readTree(String.valueOf(obj2)))){
            System.out.println("Congratulations Data Matched");
        }
        else{
            System.out.println("Mismatch Data from File1 is:");
            difference.entriesDiffering().forEach((key,value)-> System.out.println(key + ": "+ value));
        }

    }
}

