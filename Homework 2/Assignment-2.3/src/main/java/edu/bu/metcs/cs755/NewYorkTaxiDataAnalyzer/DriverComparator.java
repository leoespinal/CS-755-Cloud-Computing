package edu.bu.metcs.cs755.NewYorkTaxiDataAnalyzer;

import java.util.Comparator;

/**
 * Created by Leo Espinal on 2/28/18.
 */
public class DriverComparator implements Comparator<Driver> {
    @Override
    public int compare(Driver o1, Driver o2) {
        if(o1.getMoneyMadePerMin().get() > o2.getMoneyMadePerMin().get()) {
            return 1;
        }
        else {
            return -1;
        }
    }
}
