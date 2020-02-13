package org.apache.nifi.accumulo.processors;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class AccumuloRecordWriter extends RecordWriter<BulkIngestKey, Value> {
    private static final Logger log = Logger.getLogger(AccumuloRecordWriter.class);

    final MultiTableBatchWriter writer;


    ConcurrentHashMap<String,BatchWriter> writers = new ConcurrentHashMap<>();


    AccumuloRecordWriter(){
        writer = null;
    }
    public AccumuloRecordWriter(MultiTableBatchWriter writer){
        this.writer = writer;
    }

    LongAdder bytesWritten = new LongAdder();
    @Override
    public void write(BulkIngestKey key, Value value) throws IOException, InterruptedException {
        if (null != writer) {
            try {
                bytesWritten.add(key.getKey().getSize() + value.getSize());

                final Mutation m = getMutation(key.getKey(),value);
                BatchWriter bwriter = writers.get(key.getTableName().toString());
                if (bwriter == null) {
                    bwriter = writer.getBatchWriter(key.getTableName().toString());
                    writers.put(key.getTableName().toString(),bwriter);
                }
                bwriter.addMutation(m);
            } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
                throw new IOException(e);
            }
        }

    }

    public long getAndResetSize() { return bytesWritten.sumThenReset(); }

    /**
     * Turn a key, value into a mutation
     *
     * @param key
     * @param value
     * @return the mutation
     */
    protected Mutation getMutation(Key key, Value value) {
        Mutation m = new Mutation(key.getRow());
        if (key.isDeleted()) {
            m.putDelete(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp());
        } else {
            m.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value);
        }
        return m;
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