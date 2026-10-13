package org.jdsotomayor;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Covid_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
    // Posiciones de las columnas en owid-covid-data.csv
    private static final int LOCATION = 2;
    private static final int NEW_CASES = 5;

    private Text country = new Text();
    private LongWritable cases = new LongWritable();

    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {
        // El encabezado está en el byte 0 del archivo
        if (key.get() == 0) {
            return;
        }
        String[] fields = value.toString().split(",", -1);
        if (fields.length <= NEW_CASES) {
            return;
        }
        // Días sin casos reportados
        if (fields[NEW_CASES].isEmpty()) {
            return;
        }
        country.set(fields[LOCATION]);
        cases.set(Math.round(Double.parseDouble(fields[NEW_CASES])));
        output.collect(country, cases);
    }
}
