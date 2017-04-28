package com.assign2.inmapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Biyanta on 04/02/17.
 */
public class InMapperCombiner {

    // Mapper emits the stationID along with the object
    // containing information about sum of Tmax, sum of Tmin and count values
    public static class InMapperMeanCalcMapper
            extends Mapper<Object, Text, Text, StationClimateDetails> {

        Map<String, StationClimateDetails> partial_average;

        // initializing the data structure
        public void setup (Context context) {
            partial_average = new HashMap<String, StationClimateDetails>();
        }

        public void map(Object key, Text value, Context context) {

            String [] records = value.toString().split("\n");

            for (String record: records) {
                //Take records containing only TMAX or TMIN
                if (record.contains("TMAX") || record.contains("TMIN")) {

                    String[] recordData = record.split(",");

                    String stationID = recordData[0];

                    if (recordData[2].equals("TMIN")) {
                        // extract value of tmin
                        double tmin = Double.parseDouble(recordData[3]);

                        if (partial_average.containsKey(stationID)) {
                            // update the Tmin values as the stationID is present in the map
                            partial_average.get(stationID).updateTmin(tmin);
                        }
                        else  {
                            // enter the key-value pair in the map as stationID is not present in map
                            partial_average.put(stationID,
                                    new StationClimateDetails(tmin, 0.0, 1, 0));
                        }

                    }
                    else if (recordData[2].equals("TMAX")) {
                        // extract value of tmax
                        double tmax = Double.parseDouble(recordData[3]);

                        if (partial_average.containsKey(stationID)) {
                            // update the Tmax values as the stationID is present in the map
                            partial_average.get(stationID).updateTmax(tmax);
                        }
                        else {
                            // enter the key-value pair in the map as stationID is not present in map
                            partial_average.put(stationID,
                                    new StationClimateDetails(0.0, tmax, 0, 1));
                        }
                    }
                }
            }
        }

        // emitting the key-value pair
        public void cleanup (Context context) throws IOException, InterruptedException {

            for (String str : partial_average.keySet()) {
                context.write(new Text (str), partial_average.get(str));
            }

        }
    }

    // Reducer class
    public static class InMapperMeanCalReducer
            extends Reducer<Text,StationClimateDetails,Text,NullWritable> {

        private NullWritable result = NullWritable.get();

        public void reduce(Text key, Iterable<StationClimateDetails> values, Context context) throws IOException, InterruptedException {

            // variables to store the final result
            double sumTmin = 0.0;
            double sumTmax = 0.0;
            int countTmin = 0;
            int countTmax = 0;
            String output = "";

            // accumulating the count and the sum of tmax and tmin values for each stationID.
            for (StationClimateDetails accumulate : values) {

                sumTmin += accumulate.getSumTmin();
                countTmin += accumulate.getCountTmin();

                sumTmax += accumulate.getSumTmax();
                countTmax += accumulate.getCountTmax();

            }

            // to prevent getting NaN in the output
            if (countTmin == 0)
                output = key.toString() + ", " + "None" + ", " + (sumTmax/countTmax);

            else if (countTmax == 0)
                output = key.toString() + ", " + (sumTmin/countTmin) + ", " + "None";

            else
                output = key.toString() + ", " + (sumTmin/countTmin) + ", " + (sumTmax/countTmax);

            // Emit the key-value pair
            context.write(new Text(output), result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "In-MapperCombiner Reducer");
        job.setJarByClass(InMapperCombiner.class);
        job.setMapperClass(InMapperMeanCalcMapper.class);
        job.setReducerClass(InMapperMeanCalReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StationClimateDetails.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

