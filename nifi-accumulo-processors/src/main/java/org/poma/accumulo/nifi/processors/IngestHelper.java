package org.poma.accumulo.nifi.processors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import org.poma.accumulo.nifi.data.RecordContainer;
import org.poma.accumulo.nifi.data.RecordIngestHelper;

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
