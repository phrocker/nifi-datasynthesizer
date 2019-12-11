package org.apache.nifi.accumulo.controllerservices;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

@Tags({"accumulo", "client", "service"})
@CapabilityDescription("Provides a basic connector to Accumulo services")
public interface BaseAccumuloService extends ControllerService {


    AccumuloClient getClient();

}
