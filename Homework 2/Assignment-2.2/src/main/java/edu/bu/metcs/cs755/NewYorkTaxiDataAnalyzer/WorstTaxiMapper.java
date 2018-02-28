package edu.bu.metcs.cs755.NewYorkTaxiDataAnalyzer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Created by Leo Espinal on 2/28/18.
 */
public class WorstTaxiMapper extends Mapper<Object, Text, IntWritable, Taxi> {
    private Comparator<Taxi> comparator = new TaxiComparator();
    private PriorityQueue<Taxi> taxiPriorityQueue = new PriorityQueue<>(5, comparator);

    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
        //Parse the text file
        String [] dataPoints = line.toString().split("\t");

        //Set properties for the Taxi object
        Taxi taxi = new Taxi();
        taxi.setTaxiId(new Text(dataPoints[0]));
        taxi.setErrorRate(new DoubleWritable(Double.parseDouble(dataPoints[1])));

        //Add the taxi to the queue
        taxiPriorityQueue.add(taxi);

        //Remove smallest value
        if(taxiPriorityQueue.size() > 5) {
            taxiPriorityQueue.poll();
        }

    }

    public void cleanup(Mapper.Context context) throws IOException, InterruptedException {
        //Write taxi ID and taxi gps error count to context
        for(Taxi taxi: taxiPriorityQueue) {
            System.out.println("Mapper taxi: " + taxi.getTaxiId() + ", Error rate:" + taxi.getErrorRate());
            context.write(new IntWritable(0), taxi);
        }
    }
}
