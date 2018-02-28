package edu.bu.metcs.cs755.NewYorkTaxiDataAnalyzer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Leo Espinal on 2/28/18.
 */
public class GPSErrorMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    //Error counter
    private IntWritable errorCounter;

    //Hour of day
    private IntWritable hourOfDay;


    //Text value = csv line that needs to be parsed
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        //Store each data point in csv row in ArrayList
        ArrayList<String> csvLineItems = new ArrayList<>();

        //Tokenize string to split up values by ,
        String [] dataPoints = value.toString().split(",");
        for(String data : dataPoints) {
            csvLineItems.add(data);
        }

        //Check array list position for pick_up_date element and parse date string to get hour of day
        String pickUpDate = csvLineItems.get(2);

        //Split date into [date,time]
        String []splitDateString = pickUpDate.split(" ");

        String []pickUpTime = splitDateString[1].split(":");

        String pickUpHourString = pickUpTime[0];

        int pickUpHour = Integer.parseInt(pickUpHourString);

        //Set hour of day
        hourOfDay = new IntWritable(pickUpHour);

        //Check for gps errors
        Double pickUpLongitude = Double.parseDouble(csvLineItems.get(6));
        Double pickUpLatitude = Double.parseDouble(csvLineItems.get(7));

        Double dropOffLongitude = Double.parseDouble(csvLineItems.get(8));
        Double dropOffLatitude = Double.parseDouble(csvLineItems.get(9));

        if(pickUpLongitude == 0 || pickUpLatitude == 0|| dropOffLongitude == 0 || dropOffLatitude == 0) {
            //An error occurred in reading the GPS pick up or drop off coordinates
            errorCounter = new IntWritable(1);
        }
        else {
            //No error
            errorCounter = new IntWritable(0);
        }
        context.write(hourOfDay, errorCounter);
    }
}
