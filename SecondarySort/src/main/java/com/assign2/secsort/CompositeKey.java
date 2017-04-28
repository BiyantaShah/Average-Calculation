package com.assign2.secsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Biyanta on 07/02/17.
 */
// Composite key of stationID and year
public class CompositeKey implements WritableComparable {
    private String stationID;
    private Integer year;

    public CompositeKey() {

    }

    public CompositeKey(String id, int year) {
        super();
        this.stationID = id;
        this.year = year;
    }

    public String getStationID() {
        return stationID;
    }

    public Integer getYear() {
        return year;
    }

    @Override
    public int compareTo(Object objKeyPair) {
        CompositeKey ck = (CompositeKey)objKeyPair;

        if(this.stationID.equals(ck.stationID))
            return this.year.compareTo(ck.year);

        else
            return this.stationID.compareTo(ck.stationID);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(stationID);
        dataOutput.writeInt(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.stationID = dataInput.readUTF();
        this.year = dataInput.readInt();
    }
}
