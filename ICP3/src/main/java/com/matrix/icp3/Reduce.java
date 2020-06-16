package com.matrix.icp3;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class Reduce
        extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String[] value;
        //key=(i,k),
        //Values = [(M/N,j,V/W),..]
        HashMap<Integer, Integer> hashA = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> hashB = new HashMap<Integer, Integer>();
        for (Text val : values) {
            value = val.toString().split(",");
            if (value[0].equals("M")) {
                hashA.put(Integer.parseInt(value[1]), Integer.parseInt(value[2]));
            } else {
                hashB.put(Integer.parseInt(value[1]), Integer.parseInt(value[2]));
            }
        }
        int n = Integer.parseInt(context.getConfiguration().get("n"));
        int result = 0;
        int m_ij;
        int n_jk;
        for (int j = 0; j < n; j++) {
            m_ij = hashA.containsKey(j) ? hashA.get(j) : 0;
            n_jk = hashB.containsKey(j) ? hashB.get(j) : 0;
            result += m_ij * n_jk;
        }
        if (result != 0) {
            context.write(null,
                    new Text(key.toString() + "," + result));
        }
    }
}
