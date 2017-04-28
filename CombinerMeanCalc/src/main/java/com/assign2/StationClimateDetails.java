package com.assign2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Biyanta on 03/02/17.
 */
// This class keeps track of the sum and count of Tmax and Tmin of each stationID
public class StationClimateDetails implements Writable {
    private Double sumTmin;
    private Double sumTmax;
    private int countTmin;
    private int countTmax;

    public StationClimateDetails() { // serializes the object

    }

    public StationClimateDetails(double sumTmin, double sumTmax, int countTmin, int countTmax) {
        this.sumTmin = sumTmin;
        this.sumTmax = sumTmax;
        this.countTmin = countTmin;
        this.countTmax = countTmax;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(sumTmin);
        dataOutput.writeDouble(sumTmax);
        dataOutput.writeInt(countTmin);
        dataOutput.writeInt(countTmax);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sumTmin = dataInput.readDouble();
        this.sumTmax = dataInput.readDouble();
        this.countTmin = dataInput.readInt();
        this.countTmax = dataInput.readInt();
    }

    public Double getSumTmin() {
        return sumTmin;
    }

    public Double getSumTmax() {
        return sumTmax;
    }

    public int getCountTmin() {
        return countTmin;
    }

    public int getCountTmax() {
        return countTmax;
    }
}
