import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RevenueCount extends Configured implements Tool {
    static int printUsage() {
        System.out.println("revenuecount [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public static class RevenueCountMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        // so we don't have to do reallocations
        private final static DoubleWritable revenue = new DoubleWritable(0.0);
        private Text date = new Text();
        String expression = "^\\w*$";
        Pattern pattern = Pattern.compile(expression);
//        private final File folder = new File("s3://chrisjermainebucket/comp330_A3/");
//        private final File[] files = folder.listFiles();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
//            for (File file : files) {
//                if (file.isFile()) {
//                    List<String> rows = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
//                    for (String row : rows) {
//                        String[] array = row.split(";");
//                        date.set(array[3].substring(0, 10));
//                        revenue.set(Double.parseDouble(array[array.length - 1]));
//                        context.write(date, revenue);
//                    }
//                }
//            }
            StringTokenizer row = new StringTokenizer(value.toString(), ";");
            while (row.hasMoreTokens()) {
                String nextToken = row.nextToken();
                Matcher matcher = pattern.matcher(nextToken);
                if (!matcher.matches ())
                    continue;
                String[] data = row.toString().split(",");
                date.set(data[3].substring(0, 10));
                revenue.set(Double.parseDouble(data[data.length - 1]));
                context.write(date, revenue);
            }

        }
    }

    public static class RevenueCountReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);

        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "revenue count");
        job.setJarByClass(RevenueCount.class);
        job.setMapperClass(RevenueCountMapper.class);
        job.setCombinerClass(RevenueCountReducer.class);
        job.setReducerClass(RevenueCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {
            try {
                if ("-r".equals(args[i])) {
                    job.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Double expected instead of " + args[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " +
                        args[i-1]);
                return printUsage();
            }
        }
        // Make sure there are exactly 2 parameters left.
        if (other_args.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 2.");
            return printUsage();
        }
        FileInputFormat.setInputPaths(job, other_args.get(0));
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RevenueCount(), args);
        System.exit(res);
    }
}
