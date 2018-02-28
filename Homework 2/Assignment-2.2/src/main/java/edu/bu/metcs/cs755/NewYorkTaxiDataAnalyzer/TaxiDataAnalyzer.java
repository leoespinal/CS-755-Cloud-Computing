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
            * Jobs for task 2
            * Task 2.1 - Find the number of GPS errors per taxi
            * Task 2.2 - Find the top 5 taxi driver's taxi's that have the most gps errors
            *
            * */

            //Task 2.1
            Configuration taskTwoPartOneConfig = new Configuration();
            Job job1 = new Job(taskTwoPartOneConfig, "TaxiGpsErrorJob");
            job1.setJarByClass(TaxiDataAnalyzer.class);

            //Mapper class
            job1.setMapperClass(TaxiGPSErrorMapper.class);

            //Reducer class
            job1.setReducerClass(TaxiGPSErrorReducer.class);
            job1.setNumReduceTasks(1);

            //Output key, value classes
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(DoubleWritable.class);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(TaxiDataWritable.class);

            //Input output files
            //FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileInputFormat.addInputPath(job1, new Path("s3n://leo-espinal/hadoop-assignment-dataset/taxi-data-preprocessed.csv"));
            job1.setInputFormatClass(TextInputFormat.class);

            //FileOutputFormat.setOutputPath(job1, new Path("output_part1"));
            FileOutputFormat.setOutputPath(job1, new Path("s3n://leo-espinal/hadoop-assignment-output/second_task_output1"));
            job1.setOutputFormatClass(TextOutputFormat.class);

            job1.waitForCompletion(true);

            //Task 2.2
            Configuration taskTwoPartTwoConfig = new Configuration();
            Job job2 = new Job(taskTwoPartTwoConfig, "TopWorstTaxisJob");
            job2.setJarByClass(TaxiDataAnalyzer.class);

            //Mapper Class
            job2.setMapperClass(WorstTaxiMapper.class);

            //Reducer Class
            job2.setReducerClass(WorstTaxiReducer.class);
            job2.setNumReduceTasks(1);

            //Output key, value classes (coming out of the mapper)
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Taxi.class);

            //Input output files
            //FileInputFormat.addInputPath(job2, new Path("output_part1"));
            FileOutputFormat.setOutputPath(job2, new Path("s3n://leo-espinal/hadoop-assignment-output/second_task_output1"));
            job2.setInputFormatClass(TextInputFormat.class);

            //FileOutputFormat.setOutputPath(job2, new Path("output_part2"));
            FileOutputFormat.setOutputPath(job2, new Path("s3n://leo-espinal/hadoop-assignment-output/second_task_output2"));
            job2.setOutputFormatClass(TextOutputFormat.class);

            job2.waitForCompletion(true);

            return(job2.waitForCompletion(true) ? 0 : 1);

        } catch (InterruptedException|ClassNotFoundException|IOException e) {
            System.err.println("Error running map reduce job.");
            e.printStackTrace();
            return 2;
        }
    }
}
