package com.resolute.mapreduce.helloworld203b;

import org.apache.hadoop.io.IntWritable;

public final class Utils {
    public static String[] words(String text) {
        return text.split("\\W+");
    }


    public static int sum(Iterable<IntWritable> values) {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }
        return sum;
    }
}
