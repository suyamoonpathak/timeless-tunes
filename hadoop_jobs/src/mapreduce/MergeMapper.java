// Mapper Class
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MergeMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length == 5) {
            String songId = fields[0];
            String artistName = fields[3];
            context.write(new Text(songId), new Text("artist:" + artistName));
        }
    }
}

// Reducer Class
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MergeReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String title = "";
        String artist = "";
        for (Text value : values) {
            String[] parts = value.toString().split(":");
            if (parts[0].equals("title")) {
                title = parts[1];
            } else if (parts[0].equals("artist")) {
                artist = parts[1];
            }
        }
        if (!title.isEmpty() && !artist.isEmpty()) {
            context.write(new Text(key), new Text(title + "," + artist));
        }
    }
}
