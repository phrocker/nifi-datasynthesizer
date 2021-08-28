package org.apache.nifi.datasynthesizer.services;


import com.fasterxml.jackson.databind.JsonNode;
import org.apache.nifi.controller.ControllerService;

import java.util.Optional;


public interface SchemaLookupService extends ControllerService{
    Optional<JsonNode> sampleSchema(String key);
}