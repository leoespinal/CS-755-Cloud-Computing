package edu.bu.cs755;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


import org.apache.log4j.PropertyConfigurator;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class Main {
	public static void main(String[] args) {

		// Configure the log4j
		PropertyConfigurator.configure("log4j.properties");

		
		String bucket_name= "metcs755";
		String key_name="WikipediaPages_oneDocPerLine_1000Lines_small.txt"; 
		
		
		AmazonS3 s3Client = AmazonS3Client.builder().withRegion("us-east-1").build();

		 
		try {
			S3Object s3object = s3Client.getObject(bucket_name, key_name);
			S3ObjectInputStream s3is = s3object.getObjectContent();

		    BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));

            //Find count of each word instance
		    Map<String, Integer> wordCount = reader.lines().parallel()
		    		.map(line -> line.split(">")[1].replaceAll("<[^>]+>", "")) //
		    		.flatMap(line -> Arrays.stream(line.trim().split(" ")))
		    		.map(word -> word.replaceAll("[^a-zA-Z]", "").toUpperCase().trim())
		    		.filter(word -> word.length() > 0)
		            .map(word -> new SimpleEntry<>(word, 1)) // create a tuple of (word, 1)
					.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (v1, v2) -> v1 + v2)); // do the actual reduce by adding values with the same keys

            //Find most frequent 5000 words sorted most frequent to least frequent
			List<String> topWords = wordCount.entrySet().parallelStream()
					.sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
					.map(word -> word.getKey())
					.limit(5000)
					.collect(Collectors.toList());

            //Target words
            List<String> targetWords = Arrays.asList("during", "and", "time", "protein", "car").parallelStream().map(w -> w.toUpperCase()).collect(Collectors.toList());

            System.out.println("Scanning top words list for target words...");
            for (String element:targetWords) {
                if(topWords.contains(element)) {
                    int index = topWords.indexOf(element);
                    String word = topWords.get(index);
                    System.out.println(String.format("Index: [%d] -> Word: %s", index, word));
                }
                else {
                    //Word is not found in dictionary of top words
                    int indexNotFound = -1;
                    System.out.println(String.format("Index: [%d] -> Word: %s", indexNotFound, element));
                }
            }

			s3is.close();
			
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		} catch (IOException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}

	}

}
