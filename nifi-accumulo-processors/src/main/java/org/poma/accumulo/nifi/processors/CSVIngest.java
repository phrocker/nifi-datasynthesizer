package org.poma.accumulo.nifi.processors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.csv.config.helper.ExtendedCSVHelper;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.ingest.CSVIngestHelper;
import datawave.ingest.metadata.id.MetadataIdParser;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class CSVIngest extends RecordIngest {

    private Map<String,String> securityMarkings;
    protected Map<String,String> eventSecurityMarkingFieldDomainMap = new HashMap<>();


    @Override
    protected void checkField(String fieldName, String fieldValue, RawRecordContainer event) {
        if (requiredForValidation(fieldName)) {
            metadataForValidation.put(fieldName, fieldValue);
        }

        // If fieldName is a security marking field (as configured by EVENT_SECURITY_MARKING_FIELD_NAMES),
        // then put the marking value into this.securityMarkings, where the key is the field name for the marking
        // (as configured by EVENT_SECURITY_MARKING_FIELD_DOMAINS)
        // If fieldName is a security marking field (as configured by EVENT_SECURITY_MARKING_FIELD_NAMES),
        // then put the marking value into this.securityMarkings, where the key is the field name for the marking
        // (as configured by EVENT_SECURITY_MARKING_FIELD_DOMAINS)
        if (((ExtendedCSVHelper)getHelper()).getSecurityMarkingFieldDomainMap().containsKey(fieldName)) {
            if (null == this.securityMarkings) {
                this.securityMarkings = new HashMap<>();
            }
            if (!StringUtils.isEmpty(fieldValue)) {
                this.securityMarkings.put(((ExtendedCSVHelper)getHelper()).getSecurityMarkingFieldDomainMap().get(fieldName), fieldValue);
            }
        }
        // Now lets add metadata extracted from the parsers
        else if (!StringUtils.isEmpty(((ExtendedCSVHelper)getHelper()).getEventIdFieldName()) && fieldName.equals(((ExtendedCSVHelper)getHelper()).getEventIdFieldName())) {

            if (((ExtendedCSVHelper)getHelper()).getEventIdDowncase()) {
                fieldValue = fieldValue.toLowerCase();
            }

            // remember the id for uid creation
            eventId = fieldValue;

            try {
                getMetadataFromParsers(eventId,event);
            } catch (Exception e) {

            }
        }
        // if we set the date with id, don't overwrite it
        if (!(fieldName.equals(eventDateFieldName) && event.getDate() > 0L)) {
            super.checkField(fieldName, fieldValue,event);
        }

        overrideHelper.updateEventDataType(event, fieldName, fieldValue);
    }

    protected void getMetadataFromParsers(String idFieldValue, RawRecordContainer event) throws Exception {
        Multimap<String,String> metadata = HashMultimap.create();
        for (Map.Entry<String,MetadataIdParser> entry : ((ExtendedCSVHelper)getHelper()).getParsers().entries()) {
            entry.getValue().addMetadata(event, metadata, idFieldValue);
        }
        for (Map.Entry<String,String> entry : metadata.entries()) {
            checkField(entry.getKey(), entry.getValue(),event);
        }
    }
}
