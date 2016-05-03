package com.resolute.mapreduce.helloworld203b;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

@Ignore
public class WordCountTest {

    MapDriver<FileLineWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<FileLineWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordMapper mapper = new WordMapper();
        WordReducer reducer = new WordReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void one_line_single_word_input() throws Exception {
        mapDriver.withInput(new FileLineWritable(), new Text("test"));
        mapDriver.withOutput(new Text("test"), new IntWritable(1));
        mapDriver.runTest();
    }


    @Test
    public void two_single_word_lines() throws Exception {
        mapDriver.withInput(new FileLineWritable(), new Text("test"));
        mapDriver.withInput(new FileLineWritable(), new Text("test"));
        mapDriver.withOutput(new Text("test"), new IntWritable(1));
        mapDriver.withOutput(new Text("test"), new IntWritable(1));
        mapDriver.runTest();
    }


    @Test
    public void lowercase_everything() throws Exception {
        mapDriver.withInput(new FileLineWritable(), new Text("test"));
        mapDriver.withInput(new FileLineWritable(), new Text("Test"));
        mapDriver.withInput(new FileLineWritable(), new Text("TEST"));
        mapDriver.withOutput(new Text("test"), new IntWritable(1));
        mapDriver.withOutput(new Text("test"), new IntWritable(1));
        mapDriver.withOutput(new Text("test"), new IntWritable(1));
        mapDriver.runTest();
    }



    @Test
    public void single_two_word_different_words() throws Exception {
        mapDriver.withInput(new FileLineWritable(), new Text("test 123"));
        mapDriver.withOutput(new Text("test"), new IntWritable(1));
        mapDriver.withOutput(new Text("123"), new IntWritable(1));
        mapDriver.runTest();
    }


    @Test
    public void strip_punctuation() throws Exception {
        mapDriver.withInput(new FileLineWritable(), new Text("test? 123&maddog!"));
        mapDriver.withOutput(new Text("test"), new IntWritable(1));
        mapDriver.withOutput(new Text("123"), new IntWritable(1));
        mapDriver.withOutput(new Text("maddog"), new IntWritable(1));

        mapDriver.runTest();
    }


    @Test
    public void testReducer() throws Exception{
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("test"), values);
        reduceDriver.withOutput(new Text("test"), new IntWritable(2));
        reduceDriver.runTest();
    }

}
