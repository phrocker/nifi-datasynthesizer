package org.apache.nifi.accumulo.data;

import com.google.common.collect.Multimap;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.job.metrics.BaseMetricsReceiver;
import datawave.ingest.mapreduce.job.metrics.KeyConverter;
import datawave.ingest.mapreduce.job.metrics.Metric;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EventCountReceiver<OK,OV> extends BaseMetricsReceiver<OK,OV> {

    public EventCountReceiver() {
        super(Metric.EVENT_COUNT);
    }

    @Override
    protected String getVisibility() {
        return "MTRCS";
    }

    @Override
    protected Iterable<String> constructKeys(Metric metric, Map<String,String> labels, Multimap<String, NormalizedContentInterface> fields) {
        List<String> keys = new LinkedList<>();

        for (Map.Entry<String,NormalizedContentInterface> entry : fields.entries()) {
            String fieldAndValue = entry.getKey() + QUAL_DELIM + entry.getValue().getEventFieldValue();
            String keyStr = KeyConverter.toString(getShardId(fieldAndValue), getMetricName(), fieldAndValue, getVisibility());
            keys.add(keyStr);
        }


        return keys;
    }
}
