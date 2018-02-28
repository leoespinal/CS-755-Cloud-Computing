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
            Configuration configuration = new Configuration();

            /*
            *
            * Job to produce a list of pairs (hour_of_day, number_of_gps_errors)
            *
            * */
            Job job = new Job(configuration, "GpsErrorsPerHourJob");
            job.setJarByClass(TaxiDataAnalyzer.class);

            //Specify Mapper
            job.setMapperClass(GPSErrorMapper.class);

            //Specify Reducer
            job.setReducerClass(GPSErrorReducer.class);

            //Specify Output key value class types
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);

            //Specify input file and output file directories
            //FileInputFormat.addInputPath(job, new Path("taxi-data-preprocessed.csv"));
            FileInputFormat.addInputPath(job, new Path("s3n://leo-espinal/hadoop-assignment-dataset/taxi-data-preprocessed.csv"));
            job.setInputFormatClass(TextInputFormat.class);

            //FileOutputFormat.setOutputPath(job, new Path("first_task_output"));
            FileOutputFormat.setOutputPath(job, new Path("s3n://leo-espinal/hadoop-assignment-output/first_task_output"));
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);
            return(job.waitForCompletion(true) ? 0 : 1);

        } catch (InterruptedException|ClassNotFoundException|IOException e) {
            System.err.println("Error running map reduce job.");
            e.printStackTrace();
            return 2;
        }
    }
}