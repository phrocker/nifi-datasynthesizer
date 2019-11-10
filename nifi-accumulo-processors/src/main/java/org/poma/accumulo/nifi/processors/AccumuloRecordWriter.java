package org.poma.accumulo.nifi.processors;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;

public class AccumuloRecordWriter extends RecordWriter<Text,Mutation> {
    private static final Logger log = Logger.getLogger(AccumuloRecordWriter.class);

    final MultiTableBatchWriter writer;

    AccumuloRecordWriter(){
        writer = null;
    }
    public AccumuloRecordWriter(MultiTableBatchWriter writer){
        this.writer = writer;
    }

    @Override
    public void write(Text key, Mutation value) throws IOException, InterruptedException {
        if (null != writer) {
            try {
                writer.getBatchWriter(key.toString()).addMutation(value);
            } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (null != writer) {
            try {
                writer.close();
            } catch (MutationsRejectedException e) {
                throw new IOException(e);
            }
        }
    }

}