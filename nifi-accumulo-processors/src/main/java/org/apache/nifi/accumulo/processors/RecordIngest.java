package org.apache.nifi.accumulo.processors;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
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
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinition;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinitionConfigurationHelper;
import datawave.ingest.mapreduce.handler.edge.define.EdgeNode;
import datawave.ingest.mapreduce.job.metrics.MetricsConfiguration;
import datawave.ingest.test.StandaloneStatusReporter;
import datawave.marking.MarkingFunctions;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.nifi.accumulo.data.ContentRecordHandler;
import org.apache.nifi.accumulo.data.EdgeDataTypeHandler;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.accumulo.data.RecordContainer;
import org.apache.nifi.accumulo.data.RecordIngestHelper;
import org.apache.nifi.accumulo.data.SchemaNormalizers;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.*;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import scala.annotation.meta.field;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "accumulo", "put", "record"})
public class RecordIngest extends DatawaveAccumuloIngest {



    public RecordIngest(){}
    protected static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();


    protected static final PropertyDescriptor MEMORY_SIZE = new PropertyDescriptor.Builder()
            .name("Memory Size")
            .description("The maximum memory size Accumulo at any one time from the record set.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("10 MB")
            .build();

    protected static final PropertyDescriptor VISIBILITY_PATH = new PropertyDescriptor.Builder()
            .name("visibility-path")
            .displayName("Visibility String Record Path Root")
            .description("A record path that points to part of the record which contains a path to a mapping of visibility strings to record paths")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    protected static final PropertyDescriptor EDGE_TYPES = new PropertyDescriptor.Builder()
            .name("edge-types")
            .displayName("Edge Types")
            .description("Comma separated list that defines the edge types from which we will extract edge definitions")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    protected static final PropertyDescriptor EDGE_COLLECTION = new PropertyDescriptor.Builder()
            .name("edge-collection")
            .displayName("Edge Collection")
            .description("name of your collection of edges")
            .required(false)
            .defaultValue("MY_DATA")
            .addValidator(Validator.VALID)
            .build();

    protected static final PropertyDescriptor DEFAULT_VISIBILITY = new PropertyDescriptor.Builder()
            .name("default-visibility")
            .displayName("Default Visibility")
            .description("Default visibility when VISIBILITY_PATH is not defined. ")
            .required(false)
            .addValidator(Validator.VALID)
            .build();


    protected static final PropertyDescriptor INGEST_HELPER = new PropertyDescriptor.Builder()
            .name("Ingest Helper")
            .description("Ingest Helper class")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor POSTFIX_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("Postfix Field Names")
            .description("Determines if we post fix the array numbers or numerics. ")
            .required(false)
            .defaultValue("False").allowableValues("True","False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor POSTFIX_SEPARATOR = new PropertyDescriptor.Builder()
            .name("Postfix Separator")
            .description("Separator between sub element names")
            .required(false)
            .defaultValue("_")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor GROUPING_SEPARATOR = new PropertyDescriptor.Builder()
            .name("Grouping Separator")
            .description("Separator between grouping contexts")
            .required(false)
            .defaultValue(".")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected final TreeMap<String,String> uidOverrideFields = new TreeMap<>();

    private RecordReaderFactory recordParserFactory;


    protected UIDBuilder<UID> uidBuilder = UID.builder();


    protected String eventId;
    protected String eventDateFieldName;
    private Type type;
    private String helperClassStr;
    private AccumuloRecordWriter recordWriter;
    private DatawaveRecordReader rr = null;
    private EventMapper<LongWritable,RawRecordContainer,Text,Mutation> mapper;
    private MapContext<LongWritable,RawRecordContainer,Text,Mutation> mapContext;
    private Mapper<LongWritable, RawRecordContainer, Text, Mutation>.Context con;
    private boolean indexAllFields = false;

    protected RecordPathCache recordPathCache;
    private Boolean enableGraph = false;
    private String edgeTypes;
    private String edgeCollection;
    private boolean postFixFieldNames=false;
    private String postFixSeparator= "_";
    private String groupingSeparator = ".";

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = super.getSupportedPropertyDescriptors();
        properties.add(RECORD_READER_FACTORY);
        properties.add(MEMORY_SIZE);
        properties.add(POSTFIX_FIELD_NAMES);
        properties.add(POSTFIX_SEPARATOR);
        properties.add(GROUPING_SEPARATOR);
        properties.add(INGEST_HELPER);
        properties.add(TABLE_NAME);
        properties.add(CREATE_TABLE);
        properties.add(THREADS);
        properties.add(VISIBILITY_PATH);
        properties.add(DEFAULT_VISIBILITY);
        properties.add(EDGE_TYPES);
        properties.add(EDGE_COLLECTION);
        return properties;
    }


    Configuration conf;


    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = Collections.emptySet();
        if (validationContext.getProperty(INDEX_ALL_FIELDS).isSet() &&  validationContext.getProperty(INDEX_ALL_FIELDS).asBoolean() ){
                if (validationContext.getProperty(INDEXED_FIELDS).isSet()){
                    set.add(new ValidationResult.Builder().explanation("Indexed fields cannot be set when all fields are set to be indexed").build());
                }
        }
        return set;
    }


    SetMultimap<String, String> recordPathFromEdges = HashMultimap.create();
    SetMultimap<String, String> recordPathToEdges =  HashMultimap.create();
    Map<String, Pattern> recordPathFromRegexes = new HashMap<>();
    Map<String, Pattern> recordPathToRegexes = new HashMap<>();

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException, TableExistsException, AccumuloSecurityException, AccumuloException {

        recordParserFactory = context.getProperty(RECORD_READER_FACTORY)
                .asControllerService(RecordReaderFactory.class);

        helperClassStr = context.getProperty(INGEST_HELPER).getValue();

        postFixSeparator = context.getProperty(POSTFIX_SEPARATOR).getValue();
        groupingSeparator = context.getProperty(GROUPING_SEPARATOR).getValue();

        conf = new Configuration();


        edgeTypes = context.getProperty(EDGE_TYPES).getValue(); // get the edge types if any
        if (edgeTypes != null){
            Splitter.on(",").split(edgeTypes).forEach( x ->{
                if (context.getProperty("FROM." + x).isSet()) {

                    final String fromrel = context.getProperty("FROM." + x).getValue();
                    Splitter.on(",").split(fromrel).forEach( rel -> {
                        recordPathFromEdges.put(x, rel);
                    });
                }
                else if( context.getProperty("FROM." + x + ".regex").isSet() ){
                    recordPathFromRegexes.put(x, Pattern.compile(context.getProperty("FROM." + x + ".regex").getValue()));
                }

                if (context.getProperty("TO." + x).isSet()){
                    final String torel = context.getProperty("TO." + x).getValue();
                    Splitter.on(",").split(torel).forEach( rel -> {
                        recordPathToEdges.put(x, rel);
                    });
                }
                else if( context.getProperty("TO." + x + ".regex").isSet() ){
                    recordPathToRegexes.put(x, Pattern.compile(context.getProperty("TO." + x + ".regex").getValue()));
                }


            });

        }

        final Double maxBytes = context.getProperty(MEMORY_SIZE).asDataSize(DataUnit.B);
        postFixFieldNames = context.getProperty(POSTFIX_FIELD_NAMES).asBoolean();
        this.client = getClient(context);
        BatchWriterConfig writerConfig = new BatchWriterConfig();
        writerConfig.setMaxWriteThreads(context.getProperty(THREADS).asInteger());
        writerConfig.setMaxMemory(maxBytes.longValue());
        writerConfig.setMaxLatency(30,TimeUnit.SECONDS);
        tableWriter = client.createMultiTableBatchWriter(writerConfig);
        HashMap<String,String> flowAttributes = new HashMap<>();
        final String table = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowAttributes).getValue();

        final boolean createTables = context.getProperty(CREATE_TABLE).asBoolean();

        final String indexTable = context.getProperty(INDEX_TABLE_NAME).getValue();
        final String reverseIndexTable = context.getProperty(REVERSE_INDEX_TABLE_NAME).getValue();
        final String graphTableName = context.getProperty(GRAPH_TABLE_NAME).getValue();

        final Integer shards = context.getProperty(NUM_SHARD).asInteger();

        if ( createTables && ! client.tableOperations().exists(indexTable) ){
            client.tableOperations().create(indexTable);
            client.tableOperations().create(reverseIndexTable);
            client.tableOperations().create(table);
        }
        enableGraph = context.getProperty(ENABLE_GRAPH).asBoolean();

        if (createTables && !client.tableOperations().exists(graphTableName)){
            client.tableOperations().create(graphTableName);
        }




        final boolean enableMetadata = context.getProperty(ENABLE_METADATA).asBoolean();

        if (enableMetadata) {
            conf.set("metadata.table.name", "datawave.metadata");
            if (createTables){
                if (!client.tableOperations().exists("datawave.metadata"))
                    client.tableOperations().create("datawave.metadata");
                if (!client.tableOperations().exists("datawave.metrics"))
                    client.tableOperations().create("datawave.metrics");
            }
        }

        final boolean enableMetrics = context.getProperty(ENABLE_METRICS).asBoolean();

        if (enableMetrics) {
            final String metricsFields = context.getProperty(METRICS_FIELDS).getValue();
            final String metricsLabels = context.getProperty(LABELS_CONFIG).getValue();
            final String receiver = context.getProperty(METRICS_RECEIVERS).getValue();
            conf.set(MetricsConfiguration.METRICS_ENABLED_CONFIG, "true");
            conf.set(MetricsConfiguration.ENABLED_LABELS_CONFIG, metricsLabels);
            conf.set(MetricsConfiguration.FIELDS_CONFIG, metricsFields);
            conf.set(MetricsConfiguration.RECEIVERS_CONFIG,receiver);
            conf.set(MetricsConfiguration.METRICS_TABLE_CONFIG, "datawave.metrics");
            conf.set(MetricsConfiguration.NUM_SHARDS_CONFIG, shards.toString());

        }
        
        edgeCollection = context.getProperty(EDGE_COLLECTION).getValue();

        conf.set("num.shards", shards.toString());
        conf.set("shard.table.name", table);
        if (enableGraph){
            conf.set("protobufedge.table.name",graphTableName);
            conf.set("protobufedge.table.blacklist.enable","false");
            if (enableMetadata){
                conf.set("protobufedge.table.metadata.enable","true");
            }
            else{
                conf.set("protobufedge.table.metadata.enable","false");
            }
            conf.set(ProtobufEdgeDataTypeHandler.EDGE_SETUP_FAILURE_POLICY,"CONTINUE");
            conf.set(ProtobufEdgeDataTypeHandler.EDGE_PROCESS_FAILURE_POLICY,"CONTINUE");

            conf.set(ProtobufEdgeDataTypeHandler.EDGE_PROCESS_FAILURE_POLICY,"CONTINUE");
            conf.set(ProtobufEdgeDataTypeHandler.ACTIVITY_DATE_FUTURE_DELTA,"86400000");
            conf.set(ProtobufEdgeDataTypeHandler.ACTIVITY_DATE_PAST_DELTA,"315360000000");
            conf.set(ProtobufEdgeDataTypeHandler.EVALUATE_PRECONDITIONS,"false");
        }

        conf.set("shard.global.index.table.name", indexTable);
        conf.set("shard.global.rindex.table.name", reverseIndexTable);
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

        con = new WrappedMapper<LongWritable,RawRecordContainer,Text,Mutation>()
                .getMapContext(mapContext);

        indexAllFields = context.getProperty(INDEX_ALL_FIELDS).isSet() && context.getProperty(INDEX_ALL_FIELDS).asBoolean();
        
        try {
            mapper.setup(con);
        } catch (IOException | InterruptedException e) {
            throw new ProcessException(e);
        }

    }


    protected void checkField(String fieldName, String fieldValue, RawRecordContainer event){

    }

    @OnStopped
    public void stop() throws IOException {
        if (null != rr)
            rr.close();
        try {
            if (tableWriter != null)
                tableWriter.close();
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }

    }

    /**
     * Adapted from HBASEUtils. Their approach seemed ideal for what our intent is here.
     * @param fieldname field name visibility
     * @param flowFile flow file being written
     * @param context process context
     * @return
     */
    public static String produceFieldVisibility(String fieldname, FlowFile flowFile, ProcessContext context) {
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(fieldname)) {
            return null;
        }
        String lookupKey = String.format("visibility.%s", fieldname);
        String fromAttribute = flowFile.getAttribute(lookupKey);

        if (fromAttribute != null) {
            return fromAttribute;
        } else {
            PropertyValue descriptor = context.getProperty(lookupKey);
            if (descriptor == null || !descriptor.isSet()) {
                descriptor = context.getProperty(String.format("visibility.%s", fieldname));
            }

            String retVal = descriptor != null ? descriptor.evaluateAttributeExpressions(flowFile).getValue() : null;

            return retVal;
        }
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        final FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }

        final String dataTypeName = processContext.getProperty(DATA_NAME).evaluateAttributeExpressions(flowFile).getValue();
        
        if (!datatypes.contains(dataTypeName)){
            synchronized (this) {
                Configuration myConf =  new Configuration(conf );
                final Map<String, String> properties = processContext.getAllProperties();
                properties.forEach((x, y) -> {
                    if (null != y)
                        conf.set(x, y);
                });
                myConf.set("data.name", dataTypeName);
                myConf.set(dataTypeName + ".data.default.type.class", LcNoDiacriticsType.class.getCanonicalName());

                myConf.set(dataTypeName + ".data.header.enabled", "false");
                myConf.set(dataTypeName + ".data.separator", ",");
                myConf.set(dataTypeName + ".data.process.extra.fields", "true");
                if (processContext.getProperty(INDEX_ALL_FIELDS).isSet() && processContext.getProperty(INDEX_ALL_FIELDS).asBoolean() ){
                    myConf.set(dataTypeName + RecordIngestHelper.INDEX_ALL_FIELDS, "true");
                }

                conf.set(dataTypeName + ".data.default.type.class", LcNoDiacriticsType.class.getCanonicalName());

                List<String> canonicalHandlers = new ArrayList<>();
                canonicalHandlers.add(ContentRecordHandler.class.getName());
                canonicalHandlers.add(EdgeDataTypeHandler.class.getName());

                type = new Type(dataTypeName, IngestHelper.class, DatawaveRecordReader.class, new String[]{ContentRecordHandler.class.getName()}, 10, null);

                TypeRegistry registry = TypeRegistry.getInstance(conf);
                registry.put(dataTypeName, type);

                DataTypeHelperImpl dataTypeHelper = null;
                try {
                    dataTypeHelper = Class.forName(helperClassStr).asSubclass(DataTypeHelperImpl.class).newInstance();
                } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
                    throw new ProcessException(e);
                }
                dataTypeHelper.setup(myConf);
                datatypes.put(dataTypeName,dataTypeHelper);

                ContentRecordHandler handler = new ContentRecordHandler();

                handler.setup(new StandaloneTaskAttemptContext<LongWritable,RawRecordContainer,Text,Mutation>(myConf, new datawave.ingest.mapreduce.StandaloneStatusReporter()));

                mapper.addDataType(dataTypeName,handler);

                if (enableGraph) {
                    EdgeDataTypeHandler edgeHandler = new EdgeDataTypeHandler();

                    edgeHandler.setup(new StandaloneTaskAttemptContext<LongWritable, RawRecordContainer, Text, Mutation>(myConf, new datawave.ingest.mapreduce.StandaloneStatusReporter()));

                    mapper.addDataType(dataTypeName, edgeHandler);
                }
            }
        }

        final String recordPathText = processContext.getProperty(VISIBILITY_PATH).getValue();
        final String defaultVisibility = processContext.getProperty(DEFAULT_VISIBILITY).isSet() ? processContext.getProperty(DEFAULT_VISIBILITY).getValue() : "";

        RecordPath recordPath = null;
        if (recordPathCache == null){
            recordPathCache = new RecordPathCache(25);
        }
        if (!StringUtils.isEmpty(recordPathText)) {
            recordPath = recordPathCache.getCompiled(recordPathText);
        }





        final String indexFields = processContext.getProperty(INDEXED_FIELDS).isSet() ? "" : processContext.getProperty(INDEXED_FIELDS).evaluateAttributeExpressions(flowFile).getValue();

        Set<String> fieldsToSkip = new HashSet<>();

        final DataTypeHelperImpl dataTypeHelper = datatypes.get(dataTypeName);

        try (final InputStream in = processSession.read(flowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(flowFile, in, getLogger())) {
            int available = in.available();
            Record record;

            long events = 0;
            while ((record = reader.nextRecord()) != null) {
                int estsize =  available-in.available();
                available = in.available();
                final RecordContainer event = new RecordContainer();

                event.setSize(estsize);
                RecordField visField = null;
                String pathVisibility = null;
                if (recordPath != null) {
                    final RecordPathResult result = recordPath.evaluate(record);
                    FieldValue fv = result.getSelectedFields().findFirst().get();
                    visField = fv.getField();
                    if (null != visField)
                        fieldsToSkip.add(visField.getFieldName());
                    pathVisibility = fv.toString();
                }


                EdgeDefinitionConfigurationHelper edgeHelper = new EdgeDefinitionConfigurationHelper();

                HashSet<String> relationships = new HashSet<>(Arrays.asList("FROM","TO"));
                HashSet<String> collections = new HashSet<>(Arrays.asList(edgeCollection,"UNKONWN"));

                final Record myRecord = record;
                final List<EdgeDefinition> defs = new ArrayList<>();
                Set<String> fromKeys = new HashSet<>();
                if (!recordPathFromEdges.isEmpty()){
                    fromKeys.addAll(recordPathFromEdges.keySet());
                }
                if (!recordPathFromRegexes.isEmpty())
                    fromKeys.addAll(recordPathFromRegexes.keySet());
                Set<String> otherKeys = new HashSet<>();
                if (!recordPathToEdges.isEmpty())
                    otherKeys.addAll(recordPathToEdges.keySet());
                if (!recordPathToRegexes.isEmpty())
                    otherKeys.addAll( recordPathToRegexes.keySet());
                fromKeys.retainAll( otherKeys ); // intersect edge types.


                final Record recordRef = record;
                final Set<String> fieldNames = record.getRawFieldNames();
                fromKeys.forEach( entry -> {
                    EdgeDefinition def = new EdgeDefinition();
                    def.setEdgeType(entry);
                    def.setDirection("bi"); // bi directional edges
                    // these are the edge types.
                    List<EdgeNode> edgeNodes = new ArrayList<>();
                    final Set<String> fromFields = new HashSet<>();
                    final Set<String> toFields = new HashSet<>();
                    for(String fromEdge : recordPathFromEdges.get(entry))
                    {
                        RecordPath fromRecordPath = recordPathCache.getCompiled(fromEdge);
                        if (fromRecordPath != null) {
                            final RecordPathResult result = fromRecordPath.evaluate(myRecord);
                            FieldValue fv = result.getSelectedFields().findFirst().get();
                            RecordField fromField = fv.getField();
                            if (null != fromField) {
                                EdgeNode edgeNode = new EdgeNode();
                                edgeNode.setCollection(edgeCollection);
                                edgeNode.setRelationship("FROM");
                                edgeNode.setSelector(fromField.getFieldName());
                                fromFields.add(fromField.getFieldName());
                                edgeNodes.add(edgeNode);
                            }
                        }
                    }
                    for(String edgeType : fromKeys){

                        // get frrom
                        Pattern fromRegex = recordPathFromRegexes.get(edgeType);
                        if (fromRegex != null) {
                            final Predicate<String> acceptor = fromRegex.asMatchPredicate();
                            fieldNames.stream().filter(acceptor).forEach(

                                    x -> {
                                        if (!fromFields.contains(x)) {
                                            EdgeNode edgeNode = new EdgeNode();
                                            edgeNode.setCollection(edgeCollection);
                                            edgeNode.setRelationship("FROM");
                                            edgeNode.setSelector(x);
                                            fromFields.add(x);
                                            edgeNodes.add(edgeNode);
                                        }
                                    }
                            );
                        }
                    }

                    for(String toEdge : recordPathToEdges.get(entry)) {
                        RecordPath toRecordPath = recordPathCache.getCompiled(toEdge);
                        if (toRecordPath != null) {
                            final RecordPathResult result = toRecordPath.evaluate(myRecord);
                            FieldValue fv = result.getSelectedFields().findFirst().get();
                            RecordField fromField = fv.getField();
                            if (null != fromField) {
                                EdgeNode edgeNode = new EdgeNode();
                                edgeNode.setCollection(edgeCollection);
                                edgeNode.setRelationship("TO");
                                edgeNode.setSelector(fromField.getFieldName());
                                toFields.add(fromField.getFieldName());
                                edgeNodes.add(edgeNode);
                            }
                        }
                    }


                    for(String edgeType : fromKeys){

                        // get frrom
                        Pattern toRegex = recordPathToRegexes.get(edgeType);
                        if (toRegex != null) {
                            final Predicate<String> acceptor = toRegex.asMatchPredicate();
                            fieldNames.stream().filter(acceptor).forEach(

                                    x -> {
                                        if (!toFields.contains(x)) {
                                            EdgeNode edgeNode = new EdgeNode();
                                            edgeNode.setCollection(edgeCollection);
                                            edgeNode.setRelationship("TO");
                                            edgeNode.setSelector(x);
                                            toFields.add(x);
                                            edgeNodes.add(edgeNode);
                                        }
                                    }
                            );
                        }
                    }
                    def.setAllPairs(edgeNodes);
                    defs.add(def);
                });



                edgeHelper.setEdges(defs);
                edgeHelper.setActivityDateField("LOAD_DATE");
                edgeHelper.setEdgeAttribute2("nifi");
                edgeHelper.setEdgeAttribute3("recordingest");
                edgeHelper.init(relationships,collections);


                String visString = pathVisibility != null ? pathVisibility : defaultVisibility;

                event.setDataType(dataTypeHelper.getType());
                event.setRawFileName(flowFile.getAttribute("filename"));
                event.setRawFileTimestamp(flowFile.getEntryDate());
                event.setDate(flowFile.getEntryDate());
                event.setVisibility(visString);
                event.setEdgeConfiguration(edgeHelper);


                eventId = UUID.randomUUID().toString();

                event.setId(DataTypeOverrideHelper.getUid(eventId, event.getTimeForUID(), uidBuilder));

                ArrayList<String> indexedFields = new ArrayList<>();
                ArrayList<String> fromFields = new ArrayList<>();
                ArrayList<String> toFields = new ArrayList<>();

                if (indexAllFields) {
                    record.getSchema().getFields().stream().forEach(x ->
                    {
                        SchemaNormalizers.getNormalizers().setType(dataTypeName, x.getFieldName(), x.getDataType().getFieldType());
                        indexedFields.add(x.getFieldName().toUpperCase());
                        toFields.add(x.getFieldName().toUpperCase());
                    });


                }
                else{
                    Splitter.on(",").split(indexFields).forEach(indexedFields::add);
                }
                

                
                final Map<String,String> securityMarkings = new HashMap<>();
                final Multimap<String,String> map = HashMultimap.create();
                for (String name : reader.getSchema().getFieldNames().stream().filter(p -> !fieldsToSkip.contains(p)).collect(Collectors.toList())) {
                    Optional<RecordField> opt = record.getSchema().getField(name);
                    if (opt.isPresent() && opt.get().getDataType().getFieldType()==org.apache.nifi.serialization.record.RecordFieldType.RECORD){
                        ArrayList<String> nameQueue = new ArrayList<>();
                        expandRecord(record,(Record)record.getValue(name),opt.get().getDataType(), map,fieldsToSkip,flowFile,processContext,securityMarkings, name,indexedFields, dataTypeName, postFixFieldNames, nameQueue);
                    }   
                    else if (opt.isPresent() && opt.get().getDataType().getFieldType()==org.apache.nifi.serialization.record.RecordFieldType.ARRAY){
                        Object coercedValue = DataTypeUtils.convertType(record.getValue(name), opt.get().getDataType(), name);
                        final ArrayDataType arrayDataType = (ArrayDataType) opt.get().getDataType();
                        final DataType elementType = arrayDataType.getElementType();
                        if (coercedValue instanceof Object[]) {
                            
                            final Object[] values = (Object[]) coercedValue;
                            for (int i = 0; i < values.length; i++) {
                                ArrayList<String> nameQueue = new ArrayList<>();
                                String myName = name;
                                if (postFixFieldNames){
                                    nameQueue.add(Integer.toString(i));
                                }
                                else{
                                    myName += groupingSeparator + i;
                                }
                                final Object element = values[i];
                                expandRecord(record,element,elementType,map,fieldsToSkip,flowFile,processContext,securityMarkings, myName,indexedFields,dataTypeName, postFixFieldNames, nameQueue);
                            }
                        }
                        else{
                            ArrayList<String> nameQueue = new ArrayList<>();
                            String myName = name;
                            if (postFixFieldNames){
                                nameQueue.add("0");
                            }
                            else{
                                myName += groupingSeparator + "0";
                            }
                            expandRecord(record,coercedValue,elementType,map,fieldsToSkip,flowFile,processContext,securityMarkings, myName,indexedFields,dataTypeName,postFixFieldNames,nameQueue);
                        }
                        
                    }
                    else if (opt.isPresent() && opt.get().getDataType().getFieldType()==org.apache.nifi.serialization.record.RecordFieldType.CHOICE){
                        final DataType chosenDataType = DataTypeUtils.chooseDataType(record.getValue(name), (ChoiceDataType) opt.get().getDataType());
                            Object coercedValue = DataTypeUtils.convertType(record.getValue(name), chosenDataType, name);

                
        
                        if (coercedValue instanceof Object[]) {
                            final ArrayDataType arrayDataType = (ArrayDataType) chosenDataType;
                            final DataType elementType = arrayDataType.getElementType();
                            final Object[] values = (Object[]) coercedValue;
                            for (int i = 0; i < values.length; i++) {
                                ArrayList<String> nameQueue = new ArrayList<>();
                                String myName = name;
                                if (postFixFieldNames){
                                    nameQueue.add(Integer.toString(i));
                                }
                                else{
                                    myName += groupingSeparator + i;
                                }
                                final Object element = values[i];
                                expandRecord(record,element,elementType,map,fieldsToSkip,flowFile,processContext,securityMarkings, myName,indexedFields,dataTypeName, postFixFieldNames, nameQueue);
                            }
                        }
                        else{
                            setRecord(Optional.of(opt.get().getDataType().getFieldType()),record,name,coercedValue.toString(), map,opt.get().getDataType(),flowFile,processContext,securityMarkings,indexedFields,dataTypeName);
                        }
                        
                    }
                    else{

                        String fieldName = name.toUpperCase();
                        String recordValue = record.getAsString(name);
                        checkField(name, recordValue, event);
                        map.put(fieldName,recordValue);
                        String visibility = produceFieldVisibility(fieldName,flowFile,processContext);
                        if (!StringUtils.isEmpty(visibility)){
                            // assumes that all field with duplicate field names will hav this visibility
                            securityMarkings.put(fieldName,visibility);
                        }
                    }
                }
                if (securityMarkings.size() > 0) {
                    securityMarkings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, visString);
                    event.setSecurityMarkings(securityMarkings);
                }
                event.addIndexedFields(indexedFields);
                event.setMap(map);

                mapper.map(new LongWritable(DatawaveRecordReader.getAdder()),event,con);

                DatawaveRecordReader.incrementAdder();
                events++;
            }

            processSession.adjustCounter("RecordBytesWritten",recordWriter.getAndResetSize(),false);
            processSession.adjustCounter("RecordEventsWritten",events,false);

        } catch (Exception ex) {
            ex.printStackTrace();
            getLogger().error("Failed to put records to Accumulo.", ex);
        }
        if ( processContext.getProperty(ENABLE_METADATA).asBoolean() ||
             processContext.getProperty(ENABLE_METRICS).asBoolean()) {
            try {
                mapper.writeMetadata(con);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        processSession.transfer(flowFile,REL_SUCCESS);



    }

    private void setRecord(Optional<org.apache.nifi.serialization.record.RecordFieldType > opt ,Record record, String rawFieldName,
                            String recordValue,
                            Multimap<String,String> map,
                            DataType recordDataType,
                            FlowFile flowFile,
                            ProcessContext processContext,
                            final Map<String,String> securityMarkings, 
                            ArrayList<String> indexedFields, 
                            String dataTypeName){
        String fieldName = rawFieldName.toUpperCase();
        map.put(fieldName,recordValue);
        SchemaNormalizers.getNormalizers().setType(dataTypeName, fieldName, opt.isPresent() ? opt.get(): recordDataType.getFieldType());
        indexedFields.add(fieldName.toUpperCase());
        String visibility = produceFieldVisibility(fieldName,flowFile,processContext);
        if (!StringUtils.isEmpty(visibility)){
            // assumes that all field with duplicate field names will hav this visibility
            securityMarkings.put(fieldName,visibility);
        }
    }

    private void expandRecord(Record parentrecord,
                            Object record,DataType recordDataType,
                            Multimap<String,String> map,
                             final Set<String> fieldsToSkip,
                             FlowFile flowFile,
                             ProcessContext processContext,
                             final Map<String,String> securityMarkings,
                             String name /** parent name */,
                             ArrayList<String> indexedFields, 
                             String dataTypeName, 
                             boolean postFix, 
                             ArrayList<String> nameQueue){
        switch (recordDataType.getFieldType()) {
            case RECORD:
                    final org.apache.nifi.serialization.record.RecordSchema childSchema = ((RecordDataType)recordDataType).getChildSchema();
                    final Record rec = (Record) record;
                    for (String childName : childSchema.getFieldNames().stream().filter(p -> !fieldsToSkip.contains(p)).collect(Collectors.toList())) {
                        Optional<RecordField> opt = childSchema.getField(childName);
                        if (opt.isPresent() && opt.get().getDataType().getFieldType()==org.apache.nifi.serialization.record.RecordFieldType.RECORD){
                            
                            expandRecord(rec,rec.getValue(childName),opt.get().getDataType(),map,fieldsToSkip,flowFile,processContext,securityMarkings, name + postFixSeparator + childName,indexedFields,dataTypeName,postFix,nameQueue);
                        }
                        else if (opt.isPresent() && opt.get().getDataType().getFieldType()==org.apache.nifi.serialization.record.RecordFieldType.ARRAY){
                            Object coercedValue = DataTypeUtils.convertType(rec.getValue(childName), opt.get().getDataType(), childName);
                            final ArrayDataType arrayDataType = (ArrayDataType) opt.get().getDataType();
                            final DataType elementType = arrayDataType.getElementType();
                            if (coercedValue instanceof Object[]) {
                                
                                
                                final Object[] values = (Object[]) coercedValue;
                                for (int i = 0; i < values.length; i++) {
                                    String myName = name + postFixSeparator + childName;
                                    if (postFix && null != nameQueue){
                                        nameQueue.add(Integer.toString(i));
                                    }
                                    else{
                                        myName += groupingSeparator + i;
                                    }
                                    final Object element = values[i];
                                    expandRecord(rec,element,elementType,map,fieldsToSkip,flowFile,processContext,securityMarkings, myName,indexedFields,dataTypeName,postFix,nameQueue);
                                }
                            }
                            else{
                                String myName = name + postFixSeparator + childName;
                                if (postFix && null != nameQueue){
                                    nameQueue.add("0");
                                }
                                else{
                                    myName += groupingSeparator + "0";
                                }
                                expandRecord(rec,coercedValue,elementType,map,fieldsToSkip,flowFile,processContext,securityMarkings, myName,indexedFields,dataTypeName,postFix,nameQueue);
                            }
                            
                        }
                        else if (opt.isPresent() && opt.get().getDataType().getFieldType()==org.apache.nifi.serialization.record.RecordFieldType.CHOICE){
                            final DataType chosenDataType = DataTypeUtils.chooseDataType(rec.getValue(name), (ChoiceDataType) opt.get().getDataType());
                            Object coercedValue = DataTypeUtils.convertType(rec.getValue(name), chosenDataType, name);
            
                            if (coercedValue instanceof Object[]) {
                                final Object[] values = (Object[]) coercedValue;
                                for (int i = 0; i < values.length; i++) {
                                    String myName = name + postFixSeparator + childName;
                                    if (postFix && null != nameQueue){
                                        nameQueue.add(Integer.toString(i));
                                    }
                                    else{
                                        myName += groupingSeparator + i;
                                    }
                                    final Object element = values[i];
                                    expandRecord(rec,element,chosenDataType,map,fieldsToSkip,flowFile,processContext,securityMarkings, myName,indexedFields,dataTypeName,postFix,nameQueue);
                                }
                            }
                            else{
                                if (null != chosenDataType){
                                    /**
                                     * Field does not exist. 
                                     */
                                    String myName = name + postFixSeparator + childName;
                                    if (postFix && null != nameQueue){
                                        nameQueue.add("0");
                                    }
                                    else{
                                        myName += groupingSeparator + "0";
                                    }
                                    expandRecord(rec,coercedValue,chosenDataType,map,fieldsToSkip,flowFile,processContext,securityMarkings, myName,indexedFields,dataTypeName,postFix,nameQueue);
                                }
                            }
                            
                        }
                        else{
                            /**
                             * The schema dictates that a field should exist, but it has no value here. 
                             */
                            if (null != rec && null != rec.getAsString(childName)){
                                StringBuilder myName = new StringBuilder(name);
                                myName.append(postFixSeparator).append(childName);
                                if (postFix && null != nameQueue){
                                    for(final String nmi : nameQueue){
                                        myName.append(groupingSeparator + nmi);
                                    }
                                }
                                Optional<org.apache.nifi.serialization.record.RecordFieldType > op = Optional.empty();
                                if (opt.isPresent()){
                                    op = Optional.of(opt.get().getDataType().getFieldType());
                                }
                                setRecord(op,rec,myName.toString(),rec.getAsString(childName), map,recordDataType,flowFile,processContext,securityMarkings,indexedFields,dataTypeName);
                            }
                        }
                        }   
                break;
            case ARRAY:
                if (record instanceof Object[]) {
                    final ArrayDataType arrayDataType = (ArrayDataType) recordDataType;
                    final DataType elementType = arrayDataType.getElementType();
                    final Object[] values = (Object[]) record;
                    for (int i = 0; i < values.length; i++) {
                        String myName = name;
                        if (postFix && null != nameQueue){
                            nameQueue.add(Integer.toString(i));
                        }
                        else{
                            myName += groupingSeparator + i;
                        }
                        final Object element = values[i];
                        expandRecord(parentrecord,element,elementType,map,fieldsToSkip,flowFile,processContext,securityMarkings, myName,indexedFields,dataTypeName,postFix,nameQueue);
                    }
                }
                else{
                    StringBuilder myName = new StringBuilder(name);
                    if (postFix && null != nameQueue){
                        for(final String nmi : nameQueue){
                            myName.append(groupingSeparator + nmi );
                        }
                    }
                    if (null != record){
                        setRecord(Optional.of(recordDataType.getFieldType()),null,myName.toString(),record.toString(), map,recordDataType,flowFile,processContext,securityMarkings,indexedFields,dataTypeName);
                    }
                }
                break;
            case CHOICE:
                final DataType chosenDataType = DataTypeUtils.chooseDataType(record, (ChoiceDataType) recordDataType);
                Object coercedValue = DataTypeUtils.convertType(record, chosenDataType, name);
                if (coercedValue instanceof Object[]) {
                    final ArrayDataType arrayDataType = (ArrayDataType) chosenDataType;
                    final DataType elementType = arrayDataType.getElementType();
                    final Object[] values = (Object[]) coercedValue;
                    for (int i = 0; i < values.length; i++) {
                        String myName = name;
                        if (postFix && null != nameQueue){
                            nameQueue.add(Integer.toString(i));
                        }
                        else{
                            myName += groupingSeparator + i;
                        }
                        final Object element = values[i];
                        expandRecord(parentrecord,element,elementType,map,fieldsToSkip,flowFile,processContext,securityMarkings, myName,indexedFields,dataTypeName,postFix,nameQueue);
                    }
                }
                else{
                    StringBuilder myName = new StringBuilder(name);
                    if (postFix && null != nameQueue){
                        for(final String nmi : nameQueue){
                            myName.append(groupingSeparator + nmi );
                        }
                    }
                    if (null != record){
                        setRecord(Optional.of(recordDataType.getFieldType()),null,myName.toString(),record.toString(), map,recordDataType,flowFile,processContext,securityMarkings,indexedFields,dataTypeName);
                    }
                }
                break;
            default:
                StringBuilder myName = new StringBuilder(name);
                    if (postFix && null != nameQueue){
                        for(final String nmi : nameQueue){
                            myName.append(groupingSeparator + nmi );
                        }
                    }
                if (null != record){
                    setRecord(Optional.of(recordDataType.getFieldType()),null,myName.toString(),record.toString(), map,recordDataType,flowFile,processContext,securityMarkings,indexedFields,dataTypeName);
                }
                
            }
    }
    
}

