package edu.bu.metcs.cs755.NewYorkTaxiDataAnalyzer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Leo Espinal on 2/28/18.
 */
public class TaxiDataWritable implements Writable {
    private IntWritable gpsErrorCount;
    private IntWritable tripCount;

    public TaxiDataWritable() {
        this.gpsErrorCount = new IntWritable();
        this.tripCount = new IntWritable();
    }

    public TaxiDataWritable(IntWritable gpsErrorCount, IntWritable tripCount) {
        this.gpsErrorCount = gpsErrorCount;
        this.tripCount = tripCount;
    }

    public IntWritable getGpsErrorCount() {
        return gpsErrorCount;
    }

    public void setGpsErrorCount(IntWritable gpsErrorCount) {
        this.gpsErrorCount = gpsErrorCount;
    }

    public IntWritable getTripCount() {
        return tripCount;
    }

    public void setTripCount(IntWritable tripCount) {
        this.tripCount = tripCount;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        gpsErrorCount.readFields(dataInput);
        tripCount.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        gpsErrorCount.write(dataOutput);
        tripCount.write(dataOutput);
    }

}
