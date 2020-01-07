package org.apache.nifi.accumulo.processors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import org.apache.nifi.accumulo.data.RecordContainer;
import org.apache.nifi.accumulo.data.RecordIngestHelper;
public class IngestHelper extends RecordIngestHelper {



    public IngestHelper(){

    }

    @Override
    public Multimap<String, NormalizedContentInterface> getEventFields(RawRecordContainer value) {
        RecordContainer event = RecordContainer.class.cast(value);
        if (event != null){
            return super.normalize(event.getMap());
        }
        return HashMultimap.create();
    }
}
