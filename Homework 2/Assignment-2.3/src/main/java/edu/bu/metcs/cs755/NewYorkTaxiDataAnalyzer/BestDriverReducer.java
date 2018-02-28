package edu.bu.metcs.cs755.NewYorkTaxiDataAnalyzer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Created by Leo Espinal on 2/28/18.
 */
public class BestDriverReducer extends Reducer<IntWritable, Driver, Text, DoubleWritable> {

    private Comparator<Driver> comparator = new DriverComparator();
    private PriorityQueue<Driver> topDrivers = new PriorityQueue<>(10, comparator);

    public void reduce(IntWritable key, Iterable<Driver> drivers, Context context) throws IOException, InterruptedException {

        for(Driver driver: drivers) {
            topDrivers.add(driver);

            if(topDrivers.size() > 10) {
                topDrivers.poll();
            }
        }

        for(Driver d: topDrivers) {
            context.write(d.getDriverID(), d.getMoneyMadePerMin());
        }

    }

}
