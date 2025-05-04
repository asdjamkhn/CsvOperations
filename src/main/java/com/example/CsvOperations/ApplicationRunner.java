package com.example.CsvOperations;

import com.opencsv.exceptions.CsvValidationException;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class ApplicationRunner {

	public static void main(String[] args) throws CsvValidationException, IOException, ParseException {
		String a="F:\\SpringBoot\\CsvOperations\\data\\Map.csv";
		String b="F:\\SpringBoot\\CsvOperations\\data\\Sample1.csv";
		String c="F:\\SpringBoot\\CsvOperations\\data\\Sample2.csv";

		CsvReader csvReader = new CsvReader();
		List<Mapping> mappingList = csvReader.readMapFile(a);
		csvReader.printMappingList(mappingList);

		List<String[]> data1 = new ArrayList<>();
		List<String[]> data2 = new ArrayList<>();
		data1=csvReader.readCsvFile(b);
		data2=csvReader.readCsvFile(c);
		csvReader.csvFilesComparison(mappingList,data1,data2);

		//ReadJson Files
		String jsonfile1= "F:\\SpringBoot\\CsvOperations\\data\\TestFile1.json";
		String jsonfile2= "F:\\SpringBoot\\CsvOperations\\data\\TestFile2.json";
		csvReader.jsonComparison(jsonfile1,jsonfile2);

	}
}
