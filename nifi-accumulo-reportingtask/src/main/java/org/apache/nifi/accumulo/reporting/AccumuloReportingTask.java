package org.apache.nifi.accumulo.reporting;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.hash.UID;
import datawave.data.hash.UIDBuilder;
import datawave.data.type.LcNoDiacriticsType;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.ingest.data.config.DataTypeOverrideHelper;
import datawave.ingest.mapreduce.EventMapper;
import datawave.ingest.mapreduce.StandaloneTaskAttemptContext;
import datawave.ingest.mapreduce.handler.edge.ProtobufEdgeDataTypeHandler;
import datawave.ingest.test.StandaloneStatusReporter;
import datawave.marking.MarkingFunctions;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
 import org.apache.nifi.accumulo.data.ContentRecordHandler;
import org.apache.nifi.accumulo.data.RecordContainer;
import org.apache.nifi.accumulo.data.RecordIngestHelper;
import org.apache.nifi.accumulo.processors.AccumuloRecordWriter;
import org.apache.nifi.accumulo.processors.DatawaveRecordReader;
import org.apache.nifi.accumulo.processors.IngestHelper;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer;

import javax.json.*;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@RequiresInstanceClassLoading
@Tags({"accumulo", "client", "service"})
@CapabilityDescription("A reporting task for accumulo")
public class AccumuloReportingTask extends AbstractReportingTask {

    protected static final String LAST_EVENT_ID_KEY = "last_event_id";
    protected static final String DESTINATION_URL_PATH = "/nifi";
    protected static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";


    /***
     * Easier to set up via swagger API
     */


    protected static final PropertyDescriptor ZOOKEEPER_QUORUM = new PropertyDescriptor.Builder()
            .name("ZooKeeper Quorum")
            .description("Comma-separated list of ZooKeeper hosts for Accumulo.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final PropertyDescriptor INSTANCE_NAME = new PropertyDescriptor.Builder()
            .name("Instance Name")
            .description("Instance name of the Accumulo cluster")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    protected static final PropertyDescriptor ACCUMULO_USER = new PropertyDescriptor.Builder()
            .name("Accumulo User")
            .description("Connecting user for Accumulo")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final PropertyDescriptor ACCUMULO_PASSWORD = new PropertyDescriptor.Builder()
            .name("Accumulo Password")
            .description("Connecting user's password when using the PASSWORD Authentication type")
            .sensitive(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the Accumulo Table into which data will be placed")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CREATE_TABLE = new PropertyDescriptor.Builder()
            .name("Create Table")
            .description("Creates a table if it does not exist. This property will only be used when EL is not present in 'Table Name'")
            .required(true)
            .defaultValue("False")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    protected static final PropertyDescriptor THREADS = new PropertyDescriptor.Builder()
            .name("Threads")
            .description("Number of threads used for reading and writing")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    protected static final PropertyDescriptor MEMORY_SIZE = new PropertyDescriptor.Builder()
            .name("Memory Size")
            .description("The maximum memory size Accumulo at any one time from the record set.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("10 MB")
            .build();


    protected static final PropertyDescriptor DEFAULT_VISIBILITY = new PropertyDescriptor.Builder()
            .name("default-visibility")
            .displayName("Default Visibility")
            .description("Default visibility for provenance data.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor FILTER_EVENT_TYPE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-event-filter").displayName("Event Type to Include")
            .description("Comma-separated list of event types that will be used to filter the provenance events sent by the reporting task. "
                    + "Available event types are "
                    + Arrays.deepToString(ProvenanceEventType.values())
                    + ". If no filter is set, all the events are sent. If "
                    + "multiple filters are set, the filters are cumulative.")
            .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    static final PropertyDescriptor FILTER_EVENT_TYPE_EXCLUDE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-event-filter-exclude").displayName("Event Type to Exclude")
            .description("Comma-separated list of event types that will be used to exclude the provenance events sent by the reporting task. "
                    + "Available event types are "
                    + Arrays.deepToString(ProvenanceEventType.values())
                    + ". If no filter is set, all the events are sent. If "
                    + "multiple filters are set, the filters are cumulative. If an event type is included in Event Type to Include and excluded here, then the "
                    + "exclusion takes precedence and the event will not be sent.")
            .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    static final PropertyDescriptor FILTER_COMPONENT_TYPE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-type-filter").displayName("Component Type to Include")
            .description("Regular expression to filter the provenance events based on the component type. Only the events matching the regular "
                    + "expression will be sent. If no filter is set, all the events are sent. If multiple filters are set, the filters are cumulative.")
            .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();

    static final PropertyDescriptor FILTER_COMPONENT_TYPE_EXCLUDE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-type-filter-exclude").displayName("Component Type to Exclude")
            .description("Regular expression to exclude the provenance events based on the component type. The events matching the regular "
                    + "expression will not be sent. If no filter is set, all the events are sent. If multiple filters are set, the filters are cumulative. "
                    + "If a component type is included in Component Type to Include and excluded here, then the exclusion takes precedence and the event will not be sent.")
            .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();

    static final PropertyDescriptor FILTER_COMPONENT_ID = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-id-filter").displayName("Component ID to Include")
            .description("Comma-separated list of component UUID that will be used to filter the provenance events sent by the reporting task. If no "
                    + "filter is set, all the events are sent. If multiple filters are set, the filters are cumulative.")
            .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    static final PropertyDescriptor FILTER_COMPONENT_ID_EXCLUDE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-id-filter-exclude").displayName("Component ID to Exclude")
            .description("Comma-separated list of component UUID that will be used to exclude the provenance events sent by the reporting task. If no "
                    + "filter is set, all the events are sent. If multiple filters are set, the filters are cumulative. If a component UUID is included in "
                    + "Component ID to Include and excluded here, then the exclusion takes precedence and the event will not be sent.")
            .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    static final PropertyDescriptor FILTER_COMPONENT_NAME = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-name-filter").displayName("Component Name to Include")
            .description("Regular expression to filter the provenance events based on the component name. Only the events matching the regular "
                    + "expression will be sent. If no filter is set, all the events are sent. If multiple filters are set, the filters are cumulative.")
            .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();

    static final PropertyDescriptor FILTER_COMPONENT_NAME_EXCLUDE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-name-filter-exclude").displayName("Component Name to Exclude")
            .description("Regular expression to exclude the provenance events based on the component name. The events matching the regular "
                    + "expression will not be sent. If no filter is set, all the events are sent. If multiple filters are set, the filters are cumulative. "
                    + "If a component name is included in Component Name to Include and excluded here, then the exclusion takes precedence and the event will not be sent.")
            .required(false).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();

    static final AllowableValue BEGINNING_OF_STREAM = new AllowableValue("beginning-of-stream",
            "Beginning of Stream",
            "Start reading provenance Events from the beginning of the stream (the oldest event first)");

    static final AllowableValue END_OF_STREAM = new AllowableValue("end-of-stream", "End of Stream",
            "Start reading provenance Events from the end of the stream, ignoring old events");


    static final PropertyDescriptor START_POSITION = new PropertyDescriptor.Builder().name("start-position")
            .displayName("Start Position")
            .description("If the Reporting Task has never been run, or if its state has been reset by a user, "
                    + "specifies where in the stream of Provenance Events the Reporting Task should start")
            .allowableValues(BEGINNING_OF_STREAM, END_OF_STREAM)
            .defaultValue(BEGINNING_OF_STREAM.getValue()).required(true).build();

    static final PropertyDescriptor ALLOW_NULL_VALUES = new PropertyDescriptor.Builder().name("include-null-values")
            .displayName("Include Null Values")
            .description("Indicate if null values should be included in records. Default will be false")
            .required(true).allowableValues("true", "false").defaultValue("false").build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder().name("Batch Size")
            .displayName("Batch Size")
            .description("Specifies how many records to send in a single batch, at most.").required(true)
            .defaultValue("1000").addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();


    static final PropertyDescriptor INSTANCE_URL = new PropertyDescriptor.Builder().name("Instance URL")
            .displayName("Instance URL")
            .description("The URL of this instance to use in the Content URI of each event.").required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("http://${hostname(true)}:8080/nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    /**
     * Connector that we need to persist while we are operational.
     */
    protected AccumuloClient client;

    /**
     * Table writer that will close when we shutdown or upon error.
     */
    protected MultiTableBatchWriter tableWriter = null;
    private String table;

    private volatile ProvenanceEventConsumer eventConsumer = null;
    private Configuration conf;
    private AccumuloRecordWriter recordWriter;
    private MapContextImpl mapContext;
    private EventMapper<Object, RawRecordContainer, Object, Object> mapper;
    private DatawaveRecordReader rr;
    private Mapper.Context con;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ZOOKEEPER_QUORUM);
        properties.add(ACCUMULO_PASSWORD);
        properties.add(ACCUMULO_USER);
        properties.add(INSTANCE_NAME);
        properties.add(MEMORY_SIZE);
        properties.add(DEFAULT_VISIBILITY);
        properties.add(THREADS);
        properties.add(TABLE_NAME);
        properties.add(CREATE_TABLE);
        properties.add(FILTER_EVENT_TYPE);
        properties.add(FILTER_EVENT_TYPE_EXCLUDE);
        properties.add(FILTER_COMPONENT_TYPE);
        properties.add(FILTER_COMPONENT_TYPE_EXCLUDE);
        properties.add(FILTER_COMPONENT_ID);
        properties.add(FILTER_COMPONENT_ID_EXCLUDE);
        properties.add(FILTER_COMPONENT_NAME);
        properties.add(FILTER_COMPONENT_NAME_EXCLUDE);
        properties.add(START_POSITION);
        properties.add(ALLOW_NULL_VALUES);
        properties.add(BATCH_SIZE);
        properties.add(INSTANCE_URL);
        return properties;
    }

    public ProvenanceEventConsumer getOrCreateConsumer(final ReportingContext context) {
        if (eventConsumer != null)
            return eventConsumer;
        eventConsumer = new ProvenanceEventConsumer();
        eventConsumer.setStartPositionValue(context.getProperty(START_POSITION).getValue());
        eventConsumer.setBatchSize(context.getProperty(BATCH_SIZE).asInteger());
        eventConsumer.setLogger(getLogger());
        // initialize component type filtering
        eventConsumer.setComponentTypeRegex(
                context.getProperty(FILTER_COMPONENT_TYPE).evaluateAttributeExpressions().getValue());
        eventConsumer.setComponentTypeRegexExclude(context.getProperty(FILTER_COMPONENT_TYPE_EXCLUDE)
                .evaluateAttributeExpressions().getValue());
        eventConsumer.setComponentNameRegex(
                context.getProperty(FILTER_COMPONENT_NAME).evaluateAttributeExpressions().getValue());
        eventConsumer.setComponentNameRegexExclude(context.getProperty(FILTER_COMPONENT_NAME_EXCLUDE)
                .evaluateAttributeExpressions().getValue());

        final String[] targetEventTypes = StringUtils.stripAll(StringUtils.split(
                context.getProperty(FILTER_EVENT_TYPE).evaluateAttributeExpressions().getValue(), ','));
        if (targetEventTypes != null) {
            for (final String type : targetEventTypes) {
                try {
                    eventConsumer.addTargetEventType(ProvenanceEventType.valueOf(type));
                } catch (final Exception e) {
                    getLogger().warn(type
                            + " is not a correct event type, removed from the filtering.");
                }
            }
        }

        final String[] targetEventTypesExclude = StringUtils
                .stripAll(StringUtils.split(context.getProperty(FILTER_EVENT_TYPE_EXCLUDE)
                        .evaluateAttributeExpressions().getValue(), ','));
        if (targetEventTypesExclude != null) {
            for (final String type : targetEventTypesExclude) {
                try {
                    eventConsumer.addTargetEventTypeExclude(ProvenanceEventType.valueOf(type));
                } catch (final Exception e) {
                    getLogger().warn(type
                            + " is not a correct event type, removed from the exclude filtering.");
                }
            }
        }

        // initialize component ID filtering
        final String[] targetComponentIds = StringUtils.stripAll(StringUtils.split(
                context.getProperty(FILTER_COMPONENT_ID).evaluateAttributeExpressions().getValue(),
                ','));
        if (targetComponentIds != null) {
            eventConsumer.addTargetComponentId(targetComponentIds);
        }

        final String[] targetComponentIdsExclude = StringUtils
                .stripAll(StringUtils.split(context.getProperty(FILTER_COMPONENT_ID_EXCLUDE)
                        .evaluateAttributeExpressions().getValue(), ','));
        if (targetComponentIdsExclude != null) {
            eventConsumer.addTargetComponentIdExclude(targetComponentIdsExclude);
        }

        eventConsumer.setScheduled(true);

        return eventConsumer;
    }


    /**
     * protected static final PropertyDescriptor INGEST_HELPER = new PropertyDescriptor.Builder()
     *             .name("Ingest Helper")
     *             .description("Ingest Helper class")
     *             .required(false)
     *             .defaultValue(RecordIngestHelper.class.getCanonicalName())
     *             .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
     *             .build();
     *
     *     protected static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
     *             .name("Record Reader")
     *             .description("Hadoop Record reader class")
     *             .required(false)
     *             .defaultValue(DatawaveRecordReader.class.getCanonicalName())
     *             .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
     *             .build();
     *
     *     protected static final PropertyDescriptor DATA_HANDLER_CLASS = new PropertyDescriptor.Builder()
     *             .name("Handler Class")
     *             .description("Datawave handler class")
     *             .required(false)
     *             .defaultValue(ContentRecordHandler.class.getCanonicalName())
     *             .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
     *             .build();
     * @param context
     */

    Type provenanceType = null;

    protected UIDBuilder<UID> uidBuilder = UID.builder();

    @OnScheduled
    public void schedule(ConfigurationContext context) {
        final String instanceName = context.getProperty(INSTANCE_NAME).evaluateAttributeExpressions().getValue();
        final String zookeepers = context.getProperty(ZOOKEEPER_QUORUM).evaluateAttributeExpressions().getValue();
        final String accumuloUser = context.getProperty(ACCUMULO_USER).evaluateAttributeExpressions().getValue();

        final AuthenticationToken token = new PasswordToken(context.getProperty(ACCUMULO_PASSWORD).getValue());

        this.client = Accumulo.newClient().to(instanceName,zookeepers).as(accumuloUser,token).build();

        final Double maxBytes = context.getProperty(MEMORY_SIZE).asDataSize(DataUnit.B);

        BatchWriterConfig writerConfig = new BatchWriterConfig();
        writerConfig.setMaxWriteThreads(context.getProperty(THREADS).asInteger());
        writerConfig.setMaxMemory(maxBytes.longValue());
        writerConfig.setMaxLatency(30, TimeUnit.SECONDS);
        tableWriter = client.createMultiTableBatchWriter(writerConfig);

        table = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();

        final boolean createTables = context.getProperty(CREATE_TABLE).asBoolean();

        conf = new Configuration();

        conf.set("num.shards", "12");
        conf.set("shard.table.name", table);


        conf.set("shard.global.index.table.name", "provenanceIndex");
        conf.set("shard.global.rindex.table.name", "provenanceReverseIndex");
        conf.set("all.ingest.policy.enforcer.class", "datawave.policy.ExampleIngestPolicyEnforcer");
        conf.set("all.date.index.type.to.field.map", "LOADED=LOAD_DATE");





        OutputCommitter oc = new OutputCommitter() {
            @Override
            public void setupJob(JobContext jobContext) throws IOException {

            }

            @Override
            public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

            }

            @Override
            public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
                return false;
            }

            @Override
            public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

            }

            @Override
            public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {

            }
        };

        BlockingQueue<RawRecordContainer> queue = new ArrayBlockingQueue<>(100);

        rr = new DatawaveRecordReader(queue);


        recordWriter = new AccumuloRecordWriter(tableWriter);

        StandaloneStatusReporter sr = new StandaloneStatusReporter();
        mapper = new EventMapper<>();


        TaskAttemptID id = new TaskAttemptID("testJob", 0, TaskType.MAP, 0, 0);

        mapContext = new MapContextImpl(conf, id, rr, recordWriter, oc, sr, new InputSplit() {
            @Override
            public long getLength() throws IOException, InterruptedException {
                return 1;
            }

            @Override
            public String[] getLocations() throws IOException, InterruptedException {
                return new String[0];
            }
        });

        con = new WrappedMapper<LongWritable,RawRecordContainer, Text, Mutation>()
                .getMapContext(mapContext);
        
        try {
            mapper.setup(con);
        } catch (IOException | InterruptedException e) {
            throw new ProcessException(e);
        }
        String dataTypeName = "provenance";

        Configuration myConf =  new Configuration(conf );
        final Map<String, String> properties = context.getAllProperties();
        properties.forEach((x, y) -> {
            if (null != y)
                conf.set(x, y);
        });
        myConf.set("data.name", dataTypeName);
        myConf.set(dataTypeName + ".data.default.type.class", LcNoDiacriticsType.class.getCanonicalName());

        myConf.set(dataTypeName + ".data.header.enabled", "false");
        myConf.set(dataTypeName + ".data.separator", ",");
        myConf.set(dataTypeName + ".data.process.extra.fields", "true");

        myConf.set(dataTypeName + RecordIngestHelper.INDEX_ALL_FIELDS, "true");


        conf.set(dataTypeName + ".data.default.type.class", LcNoDiacriticsType.class.getCanonicalName());

        List<String> canonicalHandlers = new ArrayList<>();
        canonicalHandlers.add(ContentRecordHandler.class.getName());
        canonicalHandlers.add(ProtobufEdgeDataTypeHandler.class.getName());

        provenanceType = new Type(dataTypeName, IngestHelper.class, DatawaveRecordReader.class, new String[]{ContentRecordHandler.class.getName()}, 10, null);

        TypeRegistry registry = TypeRegistry.getInstance(conf);
        registry.put(dataTypeName, provenanceType);

        DataTypeHelperImpl dataTypeHelper = null;
        try {
            dataTypeHelper = RecordIngestHelper.class.asSubclass(DataTypeHelperImpl.class).newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new ProcessException(e);
        }
        dataTypeHelper.setup(myConf);

        ContentRecordHandler handler = new ContentRecordHandler();

        handler.setup(new StandaloneTaskAttemptContext<LongWritable,RawRecordContainer,Text,Mutation>(myConf, new datawave.ingest.mapreduce.StandaloneStatusReporter()));

        mapper.addDataType(dataTypeName,handler);



    }

    @OnStopped
    public void stop() throws IOException {
        rr.close();
        try {
            tableWriter.close();
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void onTrigger(ReportingContext reportingContext) {
        final boolean isClustered = reportingContext.isClustered();
        final ProcessGroupStatus procGroupStatus = reportingContext.getEventAccess().getControllerStatus();
        final String nodeId = reportingContext.getClusterNodeIdentifier();
        final String rootGroupName = procGroupStatus == null ? null : procGroupStatus.getName();
        if (nodeId == null && isClustered) {
            getLogger().debug(
                    "This instance of Apache NiFi is configured for clustering, but the Cluster Node Identifier is not yet available. "
                            + "Will wait for Node Identifier to be established.");
            return;
        }
        final String nifiUrl = reportingContext.getProperty(INSTANCE_URL).evaluateAttributeExpressions().getValue();
        URL url;
        try {
            url = new URL(nifiUrl);
        } catch (final MalformedURLException e1) {
            throw new AssertionError();
        }

        final String defaultVisibility = reportingContext.getProperty(DEFAULT_VISIBILITY).isSet() ? reportingContext.getProperty(DEFAULT_VISIBILITY).getValue() : "";
        final String hostname = url.getHost();
        final Map<String, Object> config = Collections.emptyMap();

        ProvenanceEventConsumer consumer = getOrCreateConsumer(reportingContext);
        final JsonBuilderFactory factory = Json.createBuilderFactory(config);
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        final DateFormat df = new SimpleDateFormat(TIMESTAMP_FORMAT);
        if (null != consumer){
            consumer.consumeEvents(reportingContext, (mapHolder, events) -> {

                for (final ProvenanceEventRecord evt : events) {
                    final String componentName = mapHolder.getComponentName(evt.getComponentId());
                    final String processGroupId = mapHolder.getProcessGroupId(evt.getComponentId(),
                            evt.getComponentType());
                    final String processGroupName = mapHolder.getComponentName(processGroupId);
                    final Multimap<String,String> map = HashMultimap.create();
                    final JsonObject jo = eventToJson(factory, builder, evt, df, componentName,
                            processGroupId, processGroupName, hostname, url, rootGroupName,
                            "nifi", nodeId, map);
                    final RecordContainer event = new RecordContainer();
                    String json = jo.toString();
                    event.setSize(json.length());
                    String pathVisibility = null;

                    String visString = pathVisibility != null ? pathVisibility : defaultVisibility;

                    event.setDataType(provenanceType);
                    event.setRawFileName("provenance");
                    event.setRawFileTimestamp(evt.getEventTime());
                    event.setDate(evt.getEventTime());
                    event.setVisibility(visString);
                    event.setRawData(json.getBytes());

                    String eventId = UUID.randomUUID().toString();

                    event.setId(DataTypeOverrideHelper.getUid(eventId, event.getTimeForUID(), uidBuilder));

                    ArrayList<String> indexedFields = new ArrayList<>();


                    jo.forEach( (k,y) ->
                    {
                        indexedFields.add(k.toUpperCase());
                    });

                    event.addIndexedFields(indexedFields);


                    final Map<String,String> securityMarkings = new HashMap<>();


                    securityMarkings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, visString);
                    event.setSecurityMarkings(securityMarkings);

                    event.setMap(map);

                    try {
                        mapper.map(new LongWritable(DatawaveRecordReader.getAdder()),event,con);
                    } catch (IOException | InterruptedException e) {
                        getLogger().error("Failed to publish metrics to Apache Accumulo", e);
                    }

                }
            });
            getLogger().debug("Done processing provenance data");
        }
    }

    private JsonObject eventToJson(final JsonBuilderFactory factory, final JsonObjectBuilder builder,
                                              final ProvenanceEventRecord event, final DateFormat df, final String componentName,
                                              final String processGroupId, final String processGroupName, final String hostname,
                                              final URL nifiUrl, final String applicationName, final String platform,
                                              final String nodeIdentifier, Multimap<String,String> keyValues) {
        addField(builder, "eventId", UUID.randomUUID().toString(), keyValues);
        addField(builder, "eventOrdinal", event.getEventId(), keyValues);
        addField(builder, "eventType", event.getEventType().name(), keyValues);
        addField(builder, "timestampMillis", event.getEventTime(), keyValues);
        addField(builder, "timestamp", df.format(event.getEventTime()), keyValues);
        addField(builder, "durationMillis", event.getEventDuration(), keyValues);
        addField(builder, "lineageStart", event.getLineageStartDate(), keyValues);
        addField(builder, "details", event.getDetails(), keyValues);
        addField(builder, "componentId", event.getComponentId(), keyValues);
        addField(builder, "componentType", event.getComponentType(), keyValues);
        addField(builder, "componentName", componentName, keyValues);
        addField(builder, "processGroupId", processGroupId, keyValues);
        addField(builder, "processGroupName", processGroupName, keyValues);
        addField(builder, "entityId", event.getFlowFileUuid(), keyValues);
        addField(builder, "entityType", "org.apache.nifi.flowfile.FlowFile", keyValues);
        addField(builder, "entitySize", event.getFileSize(), keyValues);
        addField(builder, "previousEntitySize", event.getPreviousFileSize(), keyValues);
        addField(builder, factory, "updatedAttributes", event.getUpdatedAttributes(), keyValues);
        addField(builder, factory, "previousAttributes", event.getPreviousAttributes(), keyValues);

        addField(builder, "actorHostname", hostname, keyValues);
        if (nifiUrl != null) {
            // TO get URL Prefix, we just remove the /nifi from the end of the URL. We know
            // that the URL ends with
            // "/nifi" because the Property Validator enforces it
            final String urlString = nifiUrl.toString();
            final String urlPrefix = urlString.substring(0,
                    urlString.length() - DESTINATION_URL_PATH.length());

            final String contentUriBase = urlPrefix + "/nifi-api/provenance-events/" + event.getEventId()
                    + "/content/";
            final String nodeIdSuffix = nodeIdentifier == null ? "" : "?clusterNodeId=" + nodeIdentifier;
            addField(builder, "contentURI", contentUriBase + "output" + nodeIdSuffix, keyValues);
            addField(builder, "previousContentURI", contentUriBase + "input" + nodeIdSuffix,
                    keyValues);
        }

        addField(builder, factory, "parentIds", event.getParentUuids(), keyValues);
        addField(builder, factory, "childIds", event.getChildUuids(), keyValues);
        addField(builder, "transitUri", event.getTransitUri(), keyValues);
        addField(builder, "remoteIdentifier", event.getSourceSystemFlowFileIdentifier(), keyValues);
        addField(builder, "alternateIdentifier", event.getAlternateIdentifierUri(), keyValues);
        addField(builder, "platform", platform, keyValues);
        addField(builder, "application", applicationName, keyValues);
        return builder.build();
    }

    public static void addField(final JsonObjectBuilder builder, final String key, final Object value,
                                Multimap<String,String> keyValues) {
        if (value != null) {
            if (value instanceof String) {
                builder.add(key, (String) value);
            } else if (value instanceof Integer) {
                builder.add(key, (Integer) value);
            } else if (value instanceof Boolean) {
                builder.add(key, (Boolean) value);
            } else if (value instanceof Long) {
                builder.add(key, (Long) value);
            } else {
                builder.add(key, value.toString());
            }

            keyValues.put(key,String.valueOf(value));
        }
    }

    public static void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key,
                                final Map<String, String> values, Multimap<String,String> keyValues) {
        if (values != null) {
            final JsonObjectBuilder mapBuilder = factory.createObjectBuilder();
            for (final Map.Entry<String, String> entry : values.entrySet()) {

                if (entry.getKey() == null) {
                    continue;
                } else if (entry.getValue() != null) {

                    mapBuilder.add(entry.getKey(), entry.getValue());
                    keyValues.put(entry.getKey(),entry.getValue());
                    keyValues.put(key + "." + entry.getKey(),entry.getValue());
                }
            }

            builder.add(key, mapBuilder);

        }
    }

    public static void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key,
                                final Collection<String> values, Multimap<String,String> keyValues) {
        if (values != null) {
            builder.add(key, createJsonArray(factory, values));
            values.stream().forEach(value -> keyValues.put(key,value));
        }
    }

    private static JsonArrayBuilder createJsonArray(JsonBuilderFactory factory, final Collection<String> values) {
        final JsonArrayBuilder builder = factory.createArrayBuilder();
        for (final String value : values) {
            if (value != null) {
                builder.add(value);
            }
        }
        return builder;
    }
}
