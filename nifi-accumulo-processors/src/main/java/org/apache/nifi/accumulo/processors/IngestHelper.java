package org.apache.nifi.accumulo.processors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import org.apache.nifi.accumulo.data.RecordContainer;
import org.apache.nifi.accumulo.data.RecordIngestHelper;

import java.util.Map;

public class IngestHelper extends RecordIngestHelper {



    public IngestHelper(){

    }

    @Override
    public Multimap<String, NormalizedContentInterface> getEventFields(RawRecordContainer value) {
        RecordContainer event = RecordContainer.class.cast(value);

        if (event != null){
            final Map<String,String> securityMarkings = event.getSecurityMarkings();
            Multimap<String, NormalizedContentInterface> retMap = super.normalize(event.getMap());
            // add the security markings only to the field name in question.
            retMap.entries().stream().forEach( x->{
                if (securityMarkings != null && securityMarkings.containsKey(x.getKey())){
                    x.getValue().setMarkings(securityMarkings);
                }
            });
            return retMap;
        }
        return HashMultimap.create();
    }
}
