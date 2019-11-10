package org.poma.accumulo.nifi.processors;

import com.google.common.collect.Multimap;
import datawave.ingest.csv.config.helper.ExtendedCSVIngestHelper;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.BaseIngestHelper;

public class IngestHelper extends ExtendedCSVIngestHelper {


    static Multimap<String,String> map;

    public static void setMap(Multimap<String,String> smap){
        map=smap;
    }


    public IngestHelper(){

    }

    @Override
    public Multimap<String, NormalizedContentInterface> getEventFields(RawRecordContainer value) {
        return super.normalize(map);
    }
}
