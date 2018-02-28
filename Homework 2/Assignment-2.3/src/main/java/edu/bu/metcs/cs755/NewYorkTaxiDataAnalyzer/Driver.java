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
public class Driver implements Writable {
    private Text driverID;
    private DoubleWritable moneyMadePerMin;

    public Driver() {
        this.driverID = new Text();
        this.moneyMadePerMin = new DoubleWritable();
    }

    public Text getDriverID() {
        return driverID;
    }

    public void setDriverID(Text driverID) {
        this.driverID = driverID;
    }

    public DoubleWritable getMoneyMadePerMin() {
        return moneyMadePerMin;
    }

    public void setMoneyMadePerMin(DoubleWritable moneyMadePerMin) {
        this.moneyMadePerMin = moneyMadePerMin;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        driverID.write(dataOutput);
        moneyMadePerMin.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        driverID.readFields(dataInput);
        moneyMadePerMin.readFields(dataInput);
    }
}
