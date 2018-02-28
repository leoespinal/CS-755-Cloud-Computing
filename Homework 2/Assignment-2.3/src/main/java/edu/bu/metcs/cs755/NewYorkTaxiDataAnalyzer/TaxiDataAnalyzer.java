package edu.bu.metcs.cs755.NewYorkTaxiDataAnalyzer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.amazonaws.AmazonServiceException;


/**
 * Created by Leo Espinal on 2/22/18.
 */
public class TaxiDataAnalyzer extends Configured implements Tool {
    public static void main(String []args) throws Exception {
        // Pre-process the taxi data
        TaxiDataAnalyzer.preProcessData();

        int status = ToolRunner.run(new Configuration(), new TaxiDataAnalyzer(), args);
        System.exit(status);
    }

    public static void preProcessData() throws IOException, AmazonServiceException {
        //Old working code
        //File taxiCsv = new File("taxi-data-sorted-small.csv");
        File taxiCsv = new File("s3n://leo-espinal/hadoop-assignment-dataset/taxi-data-sorted-small.csv");
        System.out.println("Reading taxi data file using Apache IO: ");

        List<String> lines = FileUtils.readLines(taxiCsv);
        List<String> selectedLines = new ArrayList<>();

        for(String line : lines) {
            String [] dataPoints = line.split(",");

            if(dataPoints.length != 17) {
                //there is data missing so this line should be skipped;
                continue;
            }
            else {
                //add this line to new preprocessed csv
                selectedLines.add(line);
            }
        }
        //write this line to a new file
        //File filteredDataFile = new File("taxi-data-preprocessed.csv");
        File filteredDataFile = new File("s3n://leo-espinal/hadoop-assignment-dataset/taxi-data-preprocessed.csv");
        FileUtils.writeLines(filteredDataFile, selectedLines);
    }

    public int run(String []args) {
        try {
            /*
            *
            * Job for task 3 - Top 10 best drivers based on money earned per min
            *
            * */
            Configuration taskThreeConfig = new Configuration();
            Job job3 = new Job(taskThreeConfig, "BestTaxiJob");
            job3.setJarByClass(TaxiDataAnalyzer.class);

            //Mapper Class
            job3.setMapperClass(BestDriverMapper.class);

            //Reducer Class
            job3.setReducerClass(BestDriverReducer.class);
            job3.setNumReduceTasks(1);

            //Output key, value classes
            job3.setOutputKeyClass(IntWritable.class);
            job3.setOutputValueClass(Driver.class);

            //Input output files
            //FileInputFormat.addInputPath(job3, new Path(args[0]));
            FileInputFormat.addInputPath(job3, new Path("s3n://leo-espinal/hadoop-assignment-dataset/taxi-data-preprocessed.csv"));
            job3.setInputFormatClass(TextInputFormat.class);

            //FileOutputFormat.setOutputPath(job3, new Path(args[1]));
            FileOutputFormat.setOutputPath(job3, new Path("s3n://leo-espinal/hadoop-assignment-output/third_task_output"));
            job3.setOutputFormatClass(TextOutputFormat.class);
            return(job3.waitForCompletion(true) ? 0 : 1);

        } catch (InterruptedException|ClassNotFoundException|IOException e) {
            System.err.println("Error running map reduce job.");
            e.printStackTrace();
            return 2;
        }
    }
}
