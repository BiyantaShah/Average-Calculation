import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Biyanta on 01/02/17.
 */
// This class contains information about the Tmax and Tmin for each stationID
public class StationClimateDetails implements Writable {
    private Double tmax;
    private Double tmin;
    private Boolean isTmin;

    public StationClimateDetails() { // serializes the object

    }

    public StationClimateDetails(double tmin, double tmax, boolean isTmin) {
        this.tmin = tmin;
        this.tmax = tmax;
        this.isTmin = isTmin;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(tmin);
        dataOutput.writeDouble(tmax);
        dataOutput.writeBoolean(isTmin);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.tmin = dataInput.readDouble();
        this.tmax = dataInput.readDouble();
        this.isTmin = dataInput.readBoolean();
    }

    public Double getTmin() {
        return tmin;
    }

    public Double getTmax() {
        return tmax;
    }

    public Boolean getIsTmin() {
        return isTmin;
    }

}
