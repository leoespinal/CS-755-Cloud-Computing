package edu.bu.cs755;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;



import org.apache.log4j.PropertyConfigurator;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class Main {
	
	
	 public static String extractText(String line){
	        
		 
		 
		 return "";
	 }

	public static void main(String[] args) {

		// Configure the log4j
		PropertyConfigurator.configure("log4j.properties");

		
		String bucket_name= "metcs755";
		String key_name="WikipediaPages_oneDocPerLine_1000Lines_small.txt"; 
		
		
		AmazonS3 s3Client = AmazonS3Client.builder().withRegion("us-east-1").build();

		 
		try {
			S3Object s3object = s3Client.getObject(bucket_name, key_name);
			S3ObjectInputStream s3is = s3object.getObjectContent();
			
// Some s3 file metadata printouts 			
//		    System.out.println(s3object.getObjectMetadata().getContentType());
//		    System.out.println(s3object.getObjectMetadata().getContentLength());

		    BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
		    
//		    Stream<String> lines=reader.lines().parallel();
//		    System.out.println("Number of Lines: "+ lines.count());
//		    lines.forEach(System.out::println);
		    		
		
		    
		    Map<String, Integer> wordCount=reader.lines().parallel()
		    		.map(line -> line.split(">")[1].replaceAll("<[^>]+>", "")) // 
		    		.flatMap(line -> Arrays.stream(line.trim().split(" ")))
		    		.map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim())
		    		.filter(word -> word.length() > 0)
		            .map(word -> new SimpleEntry<>(word, 1)) // create a tuple of (word, 1)
		            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (v1, v2) -> v1 + v2)); // do the actual reduce by adding values with the same keys 
		    
		    
		    
		    // Now, we want to sort and get the top 50 values. 
		    Map<String, Integer> result = wordCount.entrySet().parallelStream()
		    			.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
		    			.limit(50) // limit the output to 50 elements
		    			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));
		    
		    
		    result.forEach((k, v) -> System.out.println(String.format("%s ->  %d", k, v)));
		    
		    System.out.println(result.keySet());

		    
		    
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
