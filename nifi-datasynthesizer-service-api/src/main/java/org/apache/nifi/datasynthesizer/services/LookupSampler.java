package org.apache.nifi.datasynthesizer.services;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.nifi.datasynthesizer.SamplerBase;

import java.util.Optional;

public class LookupSampler implements SamplerBase {

    private final SchemaLookupService lookupService;
    private final String key;
    public LookupSampler(final SchemaLookupService lookupService, final String key){
        this.lookupService = lookupService;
        this.key = key;
    }

    @Override
    public Optional<JsonNode> nextSample() {
        return lookupService.sampleSchema(key);
    }
}
