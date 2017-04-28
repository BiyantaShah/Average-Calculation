package com.assign2.secsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Biyanta on 07/02/17.
 */
public class SecondarySort {

    // Mapper which emits composite key (stationID, year) as key and customized
    // class StationClimateDetails as value
    public static class SecSortMapper
            extends Mapper <Object, Text, CompositeKey, StationClimateDetails> {

        public void map (Object key, Text value, Context context) throws IOException, InterruptedException {

            String [] records = value.toString().split("\n");

            for (String record: records) {
                //Take records containing only TMAX or TMIN
                if (record.contains("TMAX") || record.contains("TMIN")) {

                    String[] recordData = record.split(",");

                    String stationID = recordData[0];
                    Integer year = Integer.parseInt(recordData[1].substring(0,4));

                    CompositeKey compositeKey = new CompositeKey(stationID, year);

                    if (recordData[2].equals("TMIN")) {
                        // extract value of tmin
                        double tmin = Double.parseDouble(recordData[3]);

                        // emit the key-value pair
                        context.write(compositeKey,
                                new StationClimateDetails(tmin, 0.0, 1, 0));
                    }
                    else if (recordData[2].equals("TMAX")) {
                        // extract value of tmax
                        double tmax = Double.parseDouble(recordData[3]);

                        // emit the key-value pair
                        context.write(compositeKey,
                                new StationClimateDetails(0.0, tmax, 0, 1));
                    }
                }
            }
        }
    }

    //Combiner class with CompositeKey and StationClimateDetails objects as input
    public static class SecSortCombiner
            extends Reducer <CompositeKey, StationClimateDetails, CompositeKey, StationClimateDetails> {

        public void reduce(CompositeKey key, Iterable<StationClimateDetails> values, Context context) throws IOException, InterruptedException {

            // variables to store the final result
            double sumTmin = 0.0;
            double sumTmax = 0.0;
            int countTmin = 0;
            int countTmax = 0;

            // accumulating the count and tmax, tmin values for each (stationID, year).
            for (StationClimateDetails accumulate : values) {

                sumTmin += accumulate.getSumTmin();
                countTmin += accumulate.getCountTmin();

                sumTmax += accumulate.getSumTmax();
                countTmax += accumulate.getCountTmax();

            }

            // Emit the same key-value pair type same as input
            context.write(key, new StationClimateDetails(sumTmin, sumTmax, countTmin, countTmax));

        }
    }

    // partitioner, tells how many reduce tasks created
    public static class SecSortBasicPartitioner
        extends Partitioner<CompositeKey, StationClimateDetails> {

        @Override
        public int getPartition(CompositeKey compositeKey, StationClimateDetails stationClimateDetails,
                                int numReduceTasks) {
            return Math.abs(compositeKey.getStationID().hashCode() % numReduceTasks);
        }
    }

    // Sorts on the stationID first and then sorts on the year
    public static class SecSortCompKeySortComparator extends WritableComparator {

        protected SecSortCompKeySortComparator() {
            super(CompositeKey.class, true);
        }

        public int compare (WritableComparable w1, WritableComparable w2){
            CompositeKey ck1 = (CompositeKey) w1;
            CompositeKey ck2 = (CompositeKey) w2;

            int comp = ck1.getStationID().compareTo(ck2.getStationID());

            if (comp != 0)
                return comp;

            return ck1.getYear().compareTo(ck2.getYear());
        }
    }

    //groups based on stationID
    public static class SecSortGroupingComparator extends WritableComparator {

        protected SecSortGroupingComparator() {
            super(CompositeKey.class, true);
        }

        public int compare (WritableComparable w1, WritableComparable w2){
            CompositeKey ck1 = (CompositeKey) w1;
            CompositeKey ck2 = (CompositeKey) w2;

            return ck1.getStationID().compareTo(ck2.getStationID());
        }

    }

    // Reducer which has input as the composite key and StationClimateDetails
    // Output would be (composite key, StationClimateDetails), but the output format
    // expected is such that we need to have all station ID’s listed for a single year. For the same
    // we use a grouping comparator, which compares objects on basis of year and
    // groups the station temperature details together, to get the desired output.
    public static class SecSortReducer
            extends Reducer<CompositeKey,StationClimateDetails,Text,NullWritable> {

        private NullWritable result = NullWritable.get();

        public void reduce (CompositeKey key, Iterable<StationClimateDetails> values, Context context) throws IOException, InterruptedException {

            // variables to store the final result
            double sumTmin = 0.0;
            double sumTmax = 0.0;
            int countTmin = 0;
            int countTmax = 0;
            StringBuffer output = new StringBuffer("");
            int prev_year = key.getYear(); // to store the previous key

            // the starting bracket appended in the output string
            output.append(key.getStationID() + ", [");

            // accumulation the count and the sum of tmax and tmin values for a particular stationID
            // in a particular year over all the years.
            for (StationClimateDetails accumulate : values) {
                if (key.getYear() != prev_year) {
                    // appending output of the previous year into the final string
                    output.append("(")
                            .append(prev_year + ", ")
                            .append(countTmin == 0 ? "None": (sumTmin/countTmin))
                            .append(", ")
                            .append(countTmax == 0 ? "None": (sumTmax/countTmax))
                            .append("), ");

                    prev_year = key.getYear();

                    // if the year is different then set new values for temperature and count
                    sumTmin = accumulate.getSumTmin();
                    countTmin = accumulate.getCountTmin();

                    sumTmax = accumulate.getSumTmax();
                    countTmax = accumulate.getCountTmax();
                }
                else {
                    // till the year is the same, add the sum of temperatures and counts
                    sumTmin += accumulate.getSumTmin();
                    countTmin += accumulate.getCountTmin();

                    sumTmax += accumulate.getSumTmax();
                    countTmax += accumulate.getCountTmax();
                }
            }
            // appending the output of the last year
            output.append("(")
                    .append(prev_year + ", ")
                    .append(countTmin == 0 ? "None": (sumTmin/countTmin))
                    .append(", ")
                    .append(countTmax == 0 ? "None": (sumTmax/countTmax))
                    .append(")]");

            // emitting the key-value pair
            context.write(new Text(output.toString()), result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Secondary sort");
        job.setJarByClass(SecondarySort.class);
        job.setMapperClass(SecSortMapper.class);
        job.setCombinerClass(SecSortCombiner.class);
        job.setReducerClass(SecSortReducer.class);
        job.setPartitionerClass(SecSortBasicPartitioner.class);
        job.setSortComparatorClass(SecSortCompKeySortComparator.class);
        job.setGroupingComparatorClass(SecSortGroupingComparator.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(StationClimateDetails.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(5); // setting the number of reduce tasks

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);
        outpath.getFileSystem(conf).delete(outpath,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
