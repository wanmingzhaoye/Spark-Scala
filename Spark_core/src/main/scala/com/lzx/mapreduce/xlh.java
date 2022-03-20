package com.lzx.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 基础数据
 *
 * @author lzx on 22/2/2022
 */
public class xlh extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            //ToolRunner 能从Configuration获取配置参数
            ToolRunner.run(new xlh(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "xlh");

        String inputPath = job.getConfiguration().get("inputPath");
        String outputPath = job.getConfiguration().get("outputPath");

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(outputPath));

        // 3 指定 mapper 输出数据的 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        // 4 指定最终输出的数据的 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //集群运行
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //小文件多用CombineTextInputFormat
        // 如果不设置 InputFormat，它默认用的是 TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值设置 4m
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 6 指定本程序的 jar 包所在的本地路径
        job.setJarByClass(xlh.class);
        job.setMapperClass(map.class);
        job.setReducerClass(reducer.class);
        return job.waitForCompletion(true)?0:1;

    }

    public class map extends Mapper<LongWritable,Text,Text, FlowBean>{

        FlowBean v;
        Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        // 1 获取一行
            String line = value.toString();
        // 2 切割字段
            String[] fields = line.split("\t");
        // 3 封装对象
        // 取出手机号码
            String phoneNum = fields[1];
            // 取出上行流量和下行流量
            long upFlow = Long.parseLong(fields[fields.length -
                    3]);
            long downFlow = Long.parseLong(fields[fields.length -
                    2]);
            k.set(phoneNum);
            v= new FlowBean(downFlow, upFlow);
            // 4 写出
            context.write(k, v);
        }

    }

    public class reducer extends Reducer<Text, FlowBean, Text, FlowBean>{
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_downFlow = 0;
        // 1 遍历所用 bean，将其中的上行流量，下行流量分别累加
            for (FlowBean flowBean : values) {
                sum_upFlow += flowBean.getUpFlow();
                sum_downFlow += flowBean.getDownFlow();
            }

        // 2 封装对象
            FlowBean resultBean = new FlowBean(sum_upFlow,
                    sum_downFlow);
        // 3 写出
            context.write(key, resultBean);
        }
    }

    //序列化
    public class FlowBean implements Writable{

        private long upFlow;
        private long downFlow;
        private long sumFlow;
        //2 反序列化时，需要反射调用空参构造函数，所以必须有
        public FlowBean(){
            super();
        }
        public FlowBean(long upFlow, long downFlow){
            super();
            this.setUpFlow(upFlow);
            this.setDownFlow(downFlow);
            this.setSumFlow(upFlow+downFlow);
        }
        //序列化
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(getUpFlow());
            dataOutput.writeLong(getDownFlow());
            dataOutput.writeLong(getSumFlow());
        }
        //4 反序列化方法
        //5 反序列化方法读顺序必须和写序列化方法的写顺序必须一致
        public void readFields(DataInput dataInput) throws IOException {
            this.setUpFlow(dataInput.readLong());
            this.setDownFlow(dataInput.readLong());
            this.setSumFlow(dataInput.readLong());

        }

        // 6 编写 toString 方法，方便后续打印到文本
        @Override
        public String toString() {
            return upFlow + "\t" + downFlow + "\t" + sumFlow;
        }

        public long getUpFlow() {
            return upFlow;
        }

        public void setUpFlow(long upFlow) {
            this.upFlow = upFlow;
        }

        public long getDownFlow() {
            return downFlow;
        }

        public void setDownFlow(long downFlow) {
            this.downFlow = downFlow;
        }

        public long getSumFlow() {
            return sumFlow;
        }

        public void setSumFlow(long sumFlow) {
            this.sumFlow = sumFlow;
        }
    }

    public class FlowBeanpx implements WritableComparable<FlowBeanpx> {

        private long upFlow;

        public int compareTo(FlowBeanpx bean) {
            int result;
            // 按照总流量大小，倒序排列 (-1在上，1在下面)
            if (sumFlow > bean.getSumFlow()) {
                result = -1;
            }else if (sumFlow < bean.getSumFlow()) {
                result = 1;
            }else {
                result = 0;
            }
            return result;
        }

        private long downFlow;
        private long sumFlow;
        //2 反序列化时，需要反射调用空参构造函数，所以必须有
        public FlowBeanpx(){
            super();
        }
        public FlowBeanpx(long upFlow, long downFlow){
            super();
            this.setUpFlow(upFlow);
            this.setDownFlow(downFlow);
            this.setSumFlow(upFlow+downFlow);
        }
        //序列化
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(getUpFlow());
            dataOutput.writeLong(getDownFlow());
            dataOutput.writeLong(getSumFlow());
        }
        //4 反序列化方法
        //5 反序列化方法读顺序必须和写序列化方法的写顺序必须一致
        public void readFields(DataInput dataInput) throws IOException {
            this.setUpFlow(dataInput.readLong());
            this.setDownFlow(dataInput.readLong());
            this.setSumFlow(dataInput.readLong());

        }

        // 6 编写 toString 方法，方便后续打印到文本
        @Override
        public String toString() {
            return upFlow + "\t" + downFlow + "\t" + sumFlow;
        }

        public long getUpFlow() {
            return upFlow;
        }

        public void setUpFlow(long upFlow) {
            this.upFlow = upFlow;
        }

        public long getDownFlow() {
            return downFlow;
        }

        public void setDownFlow(long downFlow) {
            this.downFlow = downFlow;
        }

        public long getSumFlow() {
            return sumFlow;
        }

        public void setSumFlow(long sumFlow) {
            this.sumFlow = sumFlow;
        }
    }


}

