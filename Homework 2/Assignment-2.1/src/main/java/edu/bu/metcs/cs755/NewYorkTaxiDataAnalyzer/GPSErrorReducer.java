package edu.bu.metcs.cs755.NewYorkTaxiDataAnalyzer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Leo Espinal on 2/28/18.
 */
public class GPSErrorReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable hourOfDay, Iterable<IntWritable> errorCountValues, Context context) throws IOException, InterruptedException {
        int numberOfErrors = 0;
        for(IntWritable errorCountValue: errorCountValues) {
            numberOfErrors += errorCountValue.get();
        }
        context.write(hourOfDay, new IntWritable(numberOfErrors));
    }
}
