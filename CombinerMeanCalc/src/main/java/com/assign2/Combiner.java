package com.assign2;

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

/**
 * Created by Biyanta on 03/02/17.
 */
public class Combiner {

    // Mapper emits the stationID along with the object containing
    // information about sum of Tmax, sum of Tmin and count values
    public static class CombinerMeanCalcMapper
            extends Mapper<Object, Text, Text, StationClimateDetails> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String [] records = value.toString().split("\n");

            for (String record: records) {
                //Take records containing only TMAX or TMIN
                if (record.contains("TMAX") || record.contains("TMIN")) {

                    String[] recordData = record.split(",");

                    // setting stationID extracted from the file
                    word.set(recordData[0]);

                    if (recordData[2].equals("TMIN")) {
                        // extract value of tmin
                        double tmin = Double.parseDouble(recordData[3]);

                        //emit the stationID and the corresponding object
                        context.write(word, new StationClimateDetails(tmin, 0.0, 1, 0));
                    }
                    else if (recordData[2].equals("TMAX")) {
                        // extract value of tmax
                        double tmax = Double.parseDouble(recordData[3]);

                        //emit the stationID and the corresponding object
                        context.write(word, new StationClimateDetails(0.0, tmax, 0, 1));
                    }
                }
            }
        }
    }

    // Combiner class
    public static class CombinerMeanCalCombine
            extends Reducer <Text, StationClimateDetails, Text, StationClimateDetails> {

        public void reduce(Text key, Iterable<StationClimateDetails> values, Context context) throws IOException, InterruptedException {

            // variables to store the intermediate result
            double sumTmin = 0.0;
            double sumTmax = 0.0;
            int countTmin = 0;
            int countTmax = 0;

            // accumulating the count and tmax, tmin values for each stationID.
            for (StationClimateDetails accumulate : values) {

                    sumTmin += accumulate.getSumTmin();
                    countTmin += accumulate.getCountTmin();

                    sumTmax += accumulate.getSumTmax();
                    countTmax += accumulate.getCountTmax();

            }

            // Emit the same key-value pair type as input
            context.write(key, new StationClimateDetails(sumTmin, sumTmax, countTmin, countTmax));

        }
    }

    // Reducer class
    public static class CombinerMeanCalReducer
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
        Job job = Job.getInstance(conf, "Mapper Combiner Reducer");
        job.setJarByClass(Combiner.class);
        job.setMapperClass(CombinerMeanCalcMapper.class);
        job.setCombinerClass(CombinerMeanCalCombine.class);
        job.setReducerClass(CombinerMeanCalReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StationClimateDetails.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        // to delete the output folder everytime we run the code.
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
