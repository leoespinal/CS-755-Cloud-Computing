package edu.bu.metcs.cs755.NewYorkTaxiDataAnalyzer;

import java.util.Comparator;

/**
 * Created by Leo Espinal on 2/28/18.
 */
public class TaxiComparator implements Comparator<Taxi> {
    @Override
    public int compare(Taxi o1, Taxi o2) {
        if(o1.getErrorRate().get() < o2.getErrorRate().get()) {
            return -1;
        }
        else if(o1.getErrorRate().get() > o2.getErrorRate().get()) {
            return 1;
        }
        else {
            return 0;
        }
    }
}
