package org.poma.accumulo.nifi.data;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.lucene.analysis.Analyzer;

import java.util.Map;

public class ContentRecordHandler<KEYIN> extends ContentIndexingColumnBasedHandler<KEYIN> {
    
    @Override
    public void setup(TaskAttemptContext context) {
        super.setup(context);
    }


    @Override
    protected Multimap<String,NormalizedContentInterface> getShardNamesAndValues(RawRecordContainer evt,
                                                                                 Multimap<String,NormalizedContentInterface> eventFields, boolean createGlobalIndexTerms, boolean createGlobalReverseIndexTerms,
                                                                                 StatusReporter reporter) {

        RecordContainer event = RecordContainer.class.cast(evt);
        // Reset state.
        fields = HashMultimap.create();
        index = HashMultimap.create();
        reverse = HashMultimap.create();

        Analyzer analyzer = tokenHelper.getAnalyzer();

        try {
            String lastFieldName = "";

            for (Map.Entry<String,NormalizedContentInterface> e : eventFields.entries()) {
                NormalizedContentInterface nci = e.getValue();

                // Put the normalized field name and normalized value into the index
                if (createGlobalIndexTerms) {
                    if (helper.isIndexedField(nci.getIndexedFieldName())) {
                        index.put(nci.getIndexedFieldName(), nci);
                    }
                }

                // Put the normalized field name and normalized value into the reverse
                if (createGlobalReverseIndexTerms) {
                    if (helper.isReverseIndexedField(nci.getIndexedFieldName())) {
                        NormalizedContentInterface rField = (NormalizedContentInterface) (nci.clone());
                        rField.setEventFieldValue(new StringBuilder(rField.getEventFieldValue()).reverse().toString());
                        rField.setIndexedFieldValue(new StringBuilder(rField.getIndexedFieldValue()).reverse().toString());
                        reverse.put(nci.getIndexedFieldName(), rField);
                    }
                }

                // Skip any fields that should not be included in the shard table.
                if (helper.isShardExcluded(nci.getIndexedFieldName())) {
                    continue;
                }

                // Put the event field name and original value into the fields
                fields.put(nci.getIndexedFieldName(), nci);

                String indexedFieldName = nci.getIndexedFieldName();

                // reset term position to zero if the indexed field name has changed, otherwise
                // bump the offset based on the inter-field position increment.
                if (!lastFieldName.equals(indexedFieldName)) {
                    termPosition = 0;
                    lastFieldName = indexedFieldName;
                } else {
                    termPosition = tokenHelper.getInterFieldPositionIncrement();
                }

                boolean indexField = createGlobalIndexTerms && contentHelper.isContentIndexField(indexedFieldName);
                boolean reverseIndexField = createGlobalReverseIndexTerms && contentHelper.isReverseContentIndexField(indexedFieldName);

                if (indexField || reverseIndexField) {
                    try {
                        tokenizeField(analyzer, nci, indexField, reverseIndexField, reporter);
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }

                boolean indexListField = createGlobalIndexTerms && ( contentHelper.isIndexListField(indexedFieldName) || event.isIndexedField(indexedFieldName));
                boolean reverseIndexListField = createGlobalReverseIndexTerms && ( contentHelper.isReverseIndexListField(indexedFieldName) || event.isIndexedField(indexedFieldName));

                if (indexListField || reverseIndexListField) {
                    indexListEntries(nci, indexListField, reverseIndexListField, reporter);
                }
            }
        } finally {
            analyzer.close();
        }

        validateIndexedFields(createGlobalIndexTerms, createGlobalReverseIndexTerms, reporter);

        return fields;
    }

    @Override
    public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
        return (RecordIngestHelper) helper;
    }
}
