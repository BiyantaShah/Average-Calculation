import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Biyanta on 01/02/17.
 */
public class NoCombiner {

    // Mapper emits the stationID along with the object
    // containing information about Tmax and Tmin values
    public static class MeanCalcMapper
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
                        context.write(word, new StationClimateDetails(tmin, 0.0, true));
                    }
                    else if (recordData[2].equals("TMAX")) {
                        // extract value of tmax
                        double tmax = Double.parseDouble(recordData[3]);

                        //emit the stationID and the corresponding object
                        context.write(word, new StationClimateDetails(0.0, tmax, false));
                    }
                }
            }
        }
    }

    // Reducer has the same input type as the map's output.
    public static class MeanCalReducer
            extends Reducer<Text,StationClimateDetails,Text,NullWritable> {

        private NullWritable result = NullWritable.get();

        public void reduce(Text key, Iterable<StationClimateDetails> values, Context context) throws IOException, InterruptedException {

            // variables to store the final result
            double sumTmin = 0.0;
            double sumTmax = 0.0;
            int countTmin = 0;
            int countTmax = 0;
            String output = "";

            for (StationClimateDetails accumulate : values) {
            // accessing each value from the input list and making relevant calculations
                if (accumulate.getIsTmin()) {
                    sumTmin += accumulate.getTmin();
                    countTmin += 1;
                }
                else {
                    sumTmax += accumulate.getTmax();
                    countTmax += 1;
                }
            }

            // to prevent getting NaN in the output
            if (countTmin == 0)
                output = key.toString() + ", " + "None" + ", " + (sumTmax/countTmax);

            else if (countTmax == 0)
                output = key.toString() + ", " + (sumTmin/countTmin) + ", " + "None";

            else
                output = key.toString() + ", " + (sumTmin/countTmin) + ", " + (sumTmax/countTmax);

            // Emit the key-value pair as required
            context.write(new Text(output), result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Mapper Reducer");
        job.setJarByClass(NoCombiner.class);
        job.setMapperClass(MeanCalcMapper.class);
        job.setReducerClass(MeanCalReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StationClimateDetails.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // to delete the output folder everytime we run the code.
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
