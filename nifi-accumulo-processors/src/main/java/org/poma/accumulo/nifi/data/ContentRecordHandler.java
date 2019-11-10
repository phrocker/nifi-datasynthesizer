package org.poma.accumulo.nifi.data;

import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ContentRecordHandler<KEYIN> extends ContentIndexingColumnBasedHandler<KEYIN> {
    
    @Override
    public void setup(TaskAttemptContext context) {
        super.setup(context);
    }
    
    @Override
    public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
        return (RecordIIngestHelper) helper;
    }
}
