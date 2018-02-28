package edu.bu.metcs.cs755.NewYorkTaxiDataAnalyzer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Leo Espinal on 2/28/18.
 */
public class TaxiGPSErrorReducer extends Reducer<Text, TaxiDataWritable, Text, DoubleWritable> {

    public void reduce(Text taxiID, Iterable<TaxiDataWritable> values, Context context) throws IOException, InterruptedException {
        int numberOfTrips = 0;
        int numberOfErrors = 0;

        for(TaxiDataWritable value: values) {
            numberOfErrors += value.getGpsErrorCount().get();
            numberOfTrips += value.getTripCount().get();
        }

        Double errorRate = ((double) numberOfErrors/numberOfTrips) * 100.0;

        context.write(taxiID, new DoubleWritable(errorRate));
    }

}
