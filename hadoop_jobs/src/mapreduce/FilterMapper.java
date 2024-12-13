// Mapper Class
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length == 5) {
            String songId = fields[0];
            String yearStr = fields[4];
            try {
                int year = Integer.parseInt(yearStr);
                if (year >= 1990 && year <= 2000) {
                    context.write(new Text(songId), value);
                }
            } catch (NumberFormatException e) {
                // Ignore invalid year format
            }
        }
    }
}

// Reducer Class
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, value);
        }
    }
}
