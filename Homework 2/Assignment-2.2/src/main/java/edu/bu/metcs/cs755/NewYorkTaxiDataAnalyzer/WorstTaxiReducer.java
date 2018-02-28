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
public class WorstTaxiReducer extends Reducer<IntWritable, Taxi, Text, DoubleWritable> {

    public void reduce(IntWritable key, Iterable<Taxi> taxis, Context context) throws IOException, InterruptedException {
        Comparator<Taxi> comparator = new TaxiComparator();
        PriorityQueue<Taxi> taxiPriorityQueue = new PriorityQueue<>(5, comparator);

        for(Taxi taxi: taxis) { //there is a problem with this iterator
            System.out.println("Reducer taxi: " + taxi.getTaxiId() + ", Error rate: " + taxi.getErrorRate());
            taxiPriorityQueue.add(taxi);

            if(taxiPriorityQueue.size() > 5) {
                taxiPriorityQueue.poll();
            }
        }

        for(Taxi t: taxiPriorityQueue) {
            System.out.println("Reducer taxi to context: " + t.getTaxiId() + ", Error rate: " + t.getErrorRate());
            context.write(t.getTaxiId(), t.getErrorRate());
        }

    }
}
