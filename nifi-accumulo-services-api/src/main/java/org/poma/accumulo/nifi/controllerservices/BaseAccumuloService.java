package org.poma.accumulo.nifi.controllerservices;

import org.apache.accumulo.core.client.Connector;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;

@Tags({"accumulo", "client", "service"})
@CapabilityDescription("Provides a basic connector to Accumulo services")
public interface BaseAccumuloService extends ControllerService {


    Connector getConnector();

}
