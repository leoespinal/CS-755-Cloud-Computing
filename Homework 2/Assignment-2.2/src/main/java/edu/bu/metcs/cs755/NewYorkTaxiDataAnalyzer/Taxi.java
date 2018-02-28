package edu.bu.metcs.cs755.NewYorkTaxiDataAnalyzer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Leo Espinal on 2/28/18.
 */
public class Taxi implements Writable {
    private Text taxiId;
    private DoubleWritable errorRate;

    public Taxi() {
        this.taxiId = new Text();
        this.errorRate = new DoubleWritable();
    }

    public Taxi(Text taxiId, DoubleWritable errorRate) {
        this.taxiId = taxiId;
        this.errorRate = errorRate;

    }

    public Text getTaxiId() {
        return taxiId;
    }

    public void setTaxiId(Text taxiId) {
        this.taxiId = taxiId;
    }

    public DoubleWritable getErrorRate() {
        return errorRate;
    }

    public void setErrorRate(DoubleWritable errorRate) {
        this.errorRate = errorRate;
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        taxiId.readFields(dataInput);
        errorRate.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        taxiId.write(dataOutput);
        errorRate.write(dataOutput);
    }
}
