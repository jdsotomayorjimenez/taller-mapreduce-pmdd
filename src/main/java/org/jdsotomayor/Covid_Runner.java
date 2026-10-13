package org.jdsotomayor;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import java.io.IOException;

public class Covid_Runner {
    public static void main(String[] args) throws IOException{
        JobConf conf = new JobConf(Covid_Runner.class);
        conf.setJobName("CasosCovidPorPais");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);
        conf.setMapperClass(Covid_Mapper.class);
        conf.setCombinerClass(Covid_Reducer.class);
        conf.setReducerClass(Covid_Reducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
