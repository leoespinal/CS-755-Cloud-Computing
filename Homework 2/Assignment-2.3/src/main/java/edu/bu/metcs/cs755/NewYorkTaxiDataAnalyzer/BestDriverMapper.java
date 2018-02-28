package edu.bu.metcs.cs755.NewYorkTaxiDataAnalyzer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Leo Espinal on 2/28/18.
 */
public class BestDriverMapper extends Mapper<Object, Text, IntWritable, Driver> {
    private Text driverID;

    private DoubleWritable moneyPerRideMin;

    private Comparator<Driver> comparator = new DriverComparator();

    private PriorityQueue<Driver> topDrivers = new PriorityQueue<>(10, comparator);
//    private List<Driver> list = new ArrayList<Driver>() {
//        public boolean add(Driver driver) {
//            Collections.sort(list, comparator);
//            return true;
//        }
//    };

    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
        //Store each data point in csv row in ArrayList
        ArrayList<String> csvLineItems = new ArrayList<>();

        //Tokenize string to split up values by ,
        String [] dataPoints = line.toString().split(",");
        for(String data : dataPoints) {
            csvLineItems.add(data);
        }

        //Store driver ID
        driverID = new Text(csvLineItems.get(1));

        //Store pick up time
        String pickUpDate = csvLineItems.get(2);

        //Store drop off time
        String dropOffDate = csvLineItems.get(3);

        long totalTimeInMins = 0;
        try {
            totalTimeInMins = parseDates(pickUpDate, dropOffDate);
        }
        catch (ParseException e) {
            System.err.println("Error parsing date format.");
            e.printStackTrace();
        }

        //Store amount paid to driver
        Double amountPaidToDriver = Double.parseDouble(csvLineItems.get(16));

        //Find money made per min
        Double moneyMadePerMin = amountPaidToDriver/totalTimeInMins;
        if(moneyMadePerMin.isInfinite()) {
            moneyMadePerMin = 0.0;
        }
        moneyPerRideMin = new DoubleWritable(moneyMadePerMin);

        //Create new driver object to be stored in priority queue
        Driver driver = new Driver();
        driver.setDriverID(driverID);
        driver.setMoneyMadePerMin(moneyPerRideMin);

        //list.add(driver);
        topDrivers.add(driver);

        if(topDrivers.size() > 10) {
            topDrivers.poll();
        }

    }

    public void cleanup(Mapper.Context context) throws IOException, InterruptedException {
        for(Driver driver: topDrivers) {
            context.write(new IntWritable(0), driver);
        }
    }

    public long parseDates(String pickUpDate, String dropOffDate) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss");
        Date pickUp = dateFormat.parse(pickUpDate);
        Date dropOff = dateFormat.parse(dropOffDate);
        long differenceMilliSeconds = Math.abs(dropOff.getTime() - pickUp.getTime());
        return TimeUnit.MILLISECONDS.toMinutes(differenceMilliSeconds);
    }
}
