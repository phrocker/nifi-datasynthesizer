package org.poma.accumulo.nifi.processors;

import datawave.ingest.data.RawRecordContainer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.atomic.LongAdder;

public class DatawaveRecordReader extends org.apache.hadoop.mapreduce.RecordReader<LongWritable, RawRecordContainer> {

    private static LongAdder adder = new LongAdder();

    private Queue<RawRecordContainer> containerQueue;

    public DatawaveRecordReader(Queue<RawRecordContainer> queue){
        containerQueue = queue;

    }

    public static void incrementAdder(){
        adder.add(1);
    }

    public long getAdder(){
        return adder.longValue();
    }


@Override
public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        }

@Override
public boolean nextKeyValue() throws IOException, InterruptedException {
        return containerQueue.peek()!=null;
        }

@Override
public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return new LongWritable(adder.longValue());
        }

@Override
public RawRecordContainer getCurrentValue() throws IOException, InterruptedException {
        return containerQueue.remove();
        }

@Override
public float getProgress() throws IOException, InterruptedException {
        return 100.0F;
        }

@Override
public void close() throws IOException {

        }
    }