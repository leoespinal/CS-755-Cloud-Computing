package edu.bu.metcs.cs755.NewYorkTaxiDataAnalyzer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Leo Espinal on 2/28/18.
 */
public class TaxiGPSErrorMapper extends Mapper<LongWritable, Text, Text, TaxiDataWritable> {

    //Taxi ID
    private Text taxiID;

    //Number of gps errors
    private IntWritable gpsErrorCount;

    //Number of trips
    private IntWritable tripCount = new IntWritable(1);

    //Taxi Data Writable object
    private TaxiDataWritable taxiDataWritable = new TaxiDataWritable();

    public void map(LongWritable key, Text line, Mapper.Context context) throws IOException, InterruptedException {
        //Store each data point in csv row in ArrayList
        ArrayList<String> csvLineItems = new ArrayList<>();

        //Tokenize string to split up values by ,
        String [] dataPoints = line.toString().split(",");
        for(String data : dataPoints) {
            csvLineItems.add(data);
        }

        //Store the taxi ID
        taxiID = new Text(csvLineItems.get(0));

        //Find if taxi had an error and store the count accordingly
        Double pickUpLongitude = Double.parseDouble(csvLineItems.get(6));
        Double pickUpLatitude = Double.parseDouble(csvLineItems.get(7));

        Double dropOffLongitude = Double.parseDouble(csvLineItems.get(8));
        Double dropOffLatitude = Double.parseDouble(csvLineItems.get(9));

        //Check for gps errors
        if(pickUpLongitude == 0 || pickUpLatitude == 0|| dropOffLongitude == 0 || dropOffLatitude == 0) {
            //An error occurred in reading the GPS pick up or drop off coordinates
            gpsErrorCount = new IntWritable(1);
        }
        else {
            //No error
            gpsErrorCount = new IntWritable(0);
        }

        //Set properties for taxi data writable object
        taxiDataWritable.setGpsErrorCount(gpsErrorCount);
        taxiDataWritable.setTripCount(tripCount);

        context.write(taxiID, taxiDataWritable);
    }
}
