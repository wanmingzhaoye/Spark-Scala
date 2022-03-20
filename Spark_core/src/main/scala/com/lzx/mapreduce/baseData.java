package com.lzx.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 基础数据
 *
 * @author lzx on 22/2/2022
 */
public class baseData {

    public class map extends Mapper<LongWritable,Text,Text, Text>{


        Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            context.write(k, value);
        }

    }

    public class reducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_downFlow = 0;

            context.write(key,new Text("20"));
        }
    }


}

