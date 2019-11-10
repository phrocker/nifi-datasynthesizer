package org.poma.accumulo.nifi.processors;

import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.BaseIngestHelper;

public class IngestHelper extends BaseIngestHelper {

    final BaseIngestHelper helper;
    final Multimap<String,String> map;
    public IngestHelper(BaseIngestHelper helper, final Multimap<String,String> map){
        this.helper = helper;
        this.map=map;
    }

    @Override
    public Multimap<String, NormalizedContentInterface> getEventFields(RawRecordContainer value) {
        return helper.normalize(map);
    }
}
