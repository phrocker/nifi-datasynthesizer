package org.apache.nifi.datasynthesizer;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Optional;

/**
 * Generic interface for sampler capabilities.
 */
public interface SamplerBase {

    Optional<JsonNode> nextSample();
}
