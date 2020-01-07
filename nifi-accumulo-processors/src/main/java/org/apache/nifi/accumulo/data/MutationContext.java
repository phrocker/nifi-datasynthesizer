package org.poma.accumulo.nifi.data;

import com.google.common.collect.Multimap;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.AbstractContextWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;
import java.util.Map;

public class MutationContext extends AbstractContextWriter<Text, Mutation> {

    @Override
    protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,Text,Mutation> context) throws IOException, InterruptedException {
        for (Map.Entry<BulkIngestKey,Value> entry : entries.entries()) {
            writeToContext(context, entry);
        }
    }

    protected void writeToContext(TaskInputOutputContext<?,?,Text,Mutation> context, Map.Entry<BulkIngestKey,Value> entry) throws IOException,
            InterruptedException {
        context.write(entry.getKey().getTableName(), getMutation(entry.getKey().getKey(), entry.getValue()));
    }

    Mutation m = null;


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

}
