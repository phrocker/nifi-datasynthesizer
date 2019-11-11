package org.poma.accumulo.nifi.processors;

import datawave.ingest.data.RawRecordContainer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class DatawaveRecordReader extends org.apache.hadoop.mapreduce.RecordReader<LongWritable, RawRecordContainer> {

    private static LongAdder adder = new LongAdder();

    private BlockingQueue<RawRecordContainer> containerQueue;

    private AtomicBoolean finished = new AtomicBoolean();

    public DatawaveRecordReader(BlockingQueue<RawRecordContainer> queue){
        containerQueue = queue;
        finished.set(false);

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
            // while not finished wait for data to become available;
            while(!finished.get()) {
                if (!containerQueue.isEmpty()) {
                    return true;
                }
            }
             return !containerQueue.isEmpty();
            }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return new LongWritable(adder.longValue());
            }

    @Override
    public RawRecordContainer getCurrentValue() throws IOException, InterruptedException {
            return containerQueue.take();
            }

    @Override
    public float getProgress() throws IOException, InterruptedException {
            return 100.0F;
            }

    @Override
    public void close() throws IOException {
                finished.set(true);
            }
    }