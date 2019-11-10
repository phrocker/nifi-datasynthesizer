package org.poma.accumulo.nifi.processors;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.hash.UID;
import datawave.data.hash.UIDBuilder;
import datawave.data.type.LcNoDiacriticsType;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.csv.mr.handler.ContentCSVColumnBasedHandler;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.ingest.data.config.DataTypeOverrideHelper;
import datawave.ingest.mapreduce.EventMapper;
import datawave.ingest.test.StandaloneStatusReporter;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.poma.accumulo.nifi.controllerservices.BaseAccumuloService;
import org.poma.accumulo.nifi.data.ContentRecordHandler;
import org.poma.accumulo.nifi.data.RecordContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;


@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "accumulo", "put", "record"})
public class RecordIngest extends DatawaveAccumuloIngest {

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



    protected static final PropertyDescriptor INGEST_HELPER = new PropertyDescriptor.Builder()
            .name("Ingest Helper")
            .description("Ingest Helper class")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected final TreeMap<String,String> uidOverrideFields = new TreeMap<>();

    private RecordReaderFactory recordParserFactory;


    protected UIDBuilder<UID> uidBuilder = UID.builder();


    protected String eventId;
    protected DataTypeOverrideHelper overrideHelper;
    protected String eventDateFieldName;
    private String dataTypeName;
    private Type type;


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = super.getSupportedPropertyDescriptors();
        properties.add(RECORD_READER_FACTORY);
        properties.add(MEMORY_SIZE);
        properties.add(INGEST_HELPER);
        properties.add(ACCUMULO_CONNECTOR_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(CREATE_TABLE);
        properties.add(THREADS);
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

        return set;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException, TableExistsException, AccumuloSecurityException, AccumuloException {

        recordParserFactory = context.getProperty(RECORD_READER_FACTORY)
                .asControllerService(RecordReaderFactory.class);

        final String helperClassStr = context.getProperty(INGEST_HELPER).getValue();

        dataTypeName = context.getProperty(DATA_NAME).getValue();
        
        dataTypeHelper = Class.forName(helperClassStr).asSubclass(DataTypeHelperImpl.class).newInstance();


        accumuloConnectorService = context.getProperty(ACCUMULO_CONNECTOR_SERVICE).asControllerService(BaseAccumuloService.class);
        final Double maxBytes = context.getProperty(MEMORY_SIZE).asDataSize(DataUnit.B);
        this.connector = accumuloConnectorService.getConnector();
        BatchWriterConfig writerConfig = new BatchWriterConfig();
        writerConfig.setMaxWriteThreads(context.getProperty(THREADS).asInteger());
        writerConfig.setMaxMemory(maxBytes.longValue());
        tableWriter = connector.createMultiTableBatchWriter(writerConfig);
        final String table = context.getProperty(TABLE_NAME).getValue();

        final boolean createTables = context.getProperty(CREATE_TABLE).asBoolean();

        final String indexTable = context.getProperty(INDEX_TABLE_NAME).getValue();
        final String reverseIndexTable = context.getProperty(REVERSE_INDEX_TABLE_NAME).getValue();

        final Integer shards = context.getProperty(NUM_SHARD).asInteger();

        if ( createTables && ! connector.tableOperations().exists(indexTable) ){
            connector.tableOperations().create(indexTable);
            connector.tableOperations().create(reverseIndexTable);
        }

        conf = new Configuration();
        final Map<String,String> properties = context.getAllProperties();
        properties.forEach( (x,y) ->{
            if (null != y)
                conf.set(x,y);
        });
        conf.set("data.name",dataTypeName);
        conf.set("num.shards",shards.toString());
        conf.set("shard.table.name",table);
        conf.set("csv.data.default.type.class", LcNoDiacriticsType.class.getCanonicalName());
        conf.set("shard.global.index.table.name",indexTable);
        conf.set("shard.global.rindex.table.name",reverseIndexTable);

        conf.set("all.ingest.policy.enforcer.class","datawave.policy.ExampleIngestPolicyEnforcer");
        conf.set("all.date.index.type.to.field.map","LOADED=LOAD_DATE");
        //@TODO Remove these
        conf.set("csv.data.header.enabled","false");
        conf.set("csv.data.separator",",");
        conf.set("csv.data.process.extra.fields","true");


        type = new Type(dataTypeName, IngestHelper.class, DatawaveRecordReader.class, new String[] {ContentRecordHandler.class.getName()}, 10, null);

        TypeRegistry registry = TypeRegistry.getInstance(conf);
        registry.put(dataTypeName, type);

        overrideHelper = new DataTypeOverrideHelper();
        overrideHelper.setup(conf);



        dataTypeHelper.setup(conf);

    }


    protected void checkField(String fieldName, String fieldValue, RawRecordContainer event){

    }

    protected UID uidOverride(final RawRecordContainer event) {
        if (this.uidOverrideFields.isEmpty()) {
            return null;
        }

        final StringBuilder builder = new StringBuilder();
        for (final String value : uidOverrideFields.values()) {
            builder.append(value);
        }
        return DataTypeOverrideHelper.getUid(eventId, event.getTimeForUID(), uidBuilder);
    }


    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        final FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }

        final String indexFields = processContext.getProperty(INDEXED_FIELDS).evaluateAttributeExpressions(flowFile).getValue();

        Set<String> fieldsToSkip = new HashSet<>();

        AccumuloRecordWriter recordWriter = new AccumuloRecordWriter(tableWriter);

        final RecordContainer event = new RecordContainer();

        TaskAttemptID id = new TaskAttemptID("testJob", 0, TaskType.MAP, 0, 0);

        try (final InputStream in = processSession.read(flowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(flowFile, in, getLogger())) {
            Record record;



            while ((record = reader.nextRecord()) != null) {

                event.clear();
                event.setDataType(dataTypeHelper.getType());
                event.setRawFileName(flowFile.getAttribute("filename"));
                event.setRawFileTimestamp(flowFile.getEntryDate());
                event.setDate(flowFile.getEntryDate());
                event.setVisibility("");

                ArrayList<String> indexedFields = new ArrayList<>();
                Splitter.on(",").split(indexFields).forEach(indexedFields::add);
                event.addIndexedFields(indexedFields);

                Multimap<String,String> map = HashMultimap.create();
                for (String name : reader.getSchema().getFieldNames().stream().filter(p -> !fieldsToSkip.contains(p)).collect(Collectors.toList())) {
                    String recordValue = record.getAsString(name);
                    checkField(name, recordValue, event);
                    final UID newUID = uidOverride(event);
                    if (newUID != null) {
                        event.setId(newUID);
                    } else {
                        event.generateId(null);
                    }
                    map.put(name,recordValue);
                }

                event.setMap(map);



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

                    Queue<RawRecordContainer> queue = new ArrayBlockingQueue<RawRecordContainer>(2);

                    queue.offer(event);

                    org.apache.hadoop.mapreduce.RecordReader rr = new DatawaveRecordReader(queue);


                    StandaloneStatusReporter sr = new StandaloneStatusReporter();
                    EventMapper<LongWritable,RawRecordContainer,Text,Mutation> mapper = new EventMapper<>();
                    MapContext<LongWritable,RawRecordContainer,Text,Mutation> mapContext = new MapContextImpl<>(conf, id, rr, recordWriter, oc, sr, new InputSplit() {
                        @Override
                        public long getLength() throws IOException, InterruptedException {
                            return 1;
                        }

                        @Override
                        public String[] getLocations() throws IOException, InterruptedException {
                            return new String[0];
                        }
                    });

                    Mapper<LongWritable,RawRecordContainer,Text,Mutation>.Context con = new WrappedMapper<LongWritable,RawRecordContainer,Text,Mutation>()
                            .getMapContext(mapContext);


                    mapper.run(con);
                    mapper.cleanup(con);

                    DatawaveRecordReader.incrementAdder();


            }





        } catch (Exception ex) {
            ex.printStackTrace();
            getLogger().error("Failed to put records to Accumulo.", ex);
        }
        processSession.transfer(flowFile,REL_SUCCESS);

        try {
            tableWriter.flush();
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }

    }
}


/**

 <configuration>

 <property>
 <name>file.input.format</name>
 <value>datawave.ingest.input.reader.raw.RawInputFormat</value>
 </property>

 <property>
 <name>data.name</name>
 <value>mycsv</value>
 <description>This is the name of the type of data</description>
 </property>

 <property>
 <name>mycsv.output.name</name>
 <value>csv</value>
 <description>This is the name to use on the data in Accumulo</description>
 </property>

 <property>
 <name>mycsv.ingest.helper.class</name>
 <value>datawave.ingest.csv.config.helper.ExtendedCSVIngestHelper</value>
 </property>

 <property>
 <name>mycsv.reader.class</name>
 <value>datawave.ingest.csv.mr.input.CSVRecordReader</value>
 </property>

 <property>
 <name>mycsv.handler.classes</name>
 <value>datawave.ingest.csv.mr.handler.ContentCSVColumnBasedHandler</value>
 <description>List of classes that should process this event</description>
 </property>

 <property>
 <name>mycsv.event.validators</name>
 <value></value>
 </property>

 <property>
 <name>mycsv.ingest.fatal.errors</name>
 <value>MISSING_DATA_ERROR,INVALID_DATA_ERROR</value>
 <description>This is the list of comma delimited additional RawDataError enumerations that we consider fatal.
 MISSING_DATA_ERROR: Missing required fields
 INVALID_DATA_ERROR: Found a value in a range of invalid values
 </description>
 </property>

 <property>
 <name>mycsv.ingest.ignorable.error.helpers</name>
 <value>datawave.ingest.data.config.ingest.IgnorableFatalErrorHelper</value>
 </property>

 <property>
 <name>mycsv.ingest.ignorable.fatal.errors</name>
 <value>MISSING_DATA_ERROR,INVALID_DATA_ERROR</value>
 <description>This is the list of comma delimited RawDataError values that result in dropping the event
 instead of redirecting to the processingErrors table. Used by the IgnorableFatalErrorHelper.
 </description>
 </property>

 <property>
 <name>mycsv.data.category.uuid.fields</name>
 <value>UUID,PARENT_UUID</value>
 <description>List of fields that contain UUIDs</description>
 </property>

 <property>
 <name>mycsv.data.separator</name>
 <value>,</value>
 <description>This is the separator to use for delimited text, and between configuration file parameters with multiple values.
 </description>
 </property>

 <property>
 <name>mycsv.data.default.normalization.failure.policy</name>
 <value>DROP</value>
 <description>For normalization failures: DROP, LEAVE, FAIL</description>
 </property>

 <property>
 <name>mycsv.data.category.id.field</name>
 <value>EVENT_ID</value>
 <description>Name of the field that contains the id</description>
 </property>

 <property>
 <name>mycsv.data.header</name>
 <value>PROCESSING_DATE,EVENT_ID,LANGUAGE,ORIGINAL_SIZE,PROCESSED_SIZE,FILE_TYPE,MD5,SHA1,SHA256,EVENT_DATE,SECURITY_MARKING</value>
 <description>These are the fields in the raw data (delimited by separator).</description>
 </property>

 <property>
 <name>mycsv.data.category.marking.default</name>
 <value>PRIVATE</value>
 <description>Default ColumnVisibility to be applied to fields/records if none provided in the data</description>
 </property>

 <property>
 <name>mycsv.data.category.security.field.names</name>
 <value>SECURITY_MARKING</value>
 <description>These are the fields in the raw data that contain the event's security markings.
 Item N in this comma-delimited list must have a corresponding item N in the *.data.category.security.field.domains list
 </description>
 </property>

 <property>
 <name>mycsv.data.category.security.field.domains</name>
 <value>columnVisibility</value>
 <description>Marking domain N in this comma-delimited list corresponds to the security marking
 field N configured in the *.data.category.security.field.names list
 </description>
 </property>

 <property>
 <name>mycsv.data.process.extra.fields</name>
 <value>true</value>
 <description>Are fields past the header to be processed as name=value pairs.</description>
 </property>

 <property>
 <name>mycsv.data.field.drop</name>
 <value></value>
 </property>

 <!-- default is to treat all fields as multivalued, this is the blacklist -->
 <property>
 <name>mycsv.data.fields.multivalued.blacklist</name>
 <value>RAW_TEXT_BLOB</value>
 </property>

 <property>
 <name>mycsv.data.multivalued.separator</name>
 <value>;</value>
 </property>

 <property>
 <name>mycsv.data.multivalued.threshold</name>
 <value>100000</value>
 </property>

 <property>
 <name>mycsv.data.multivalued.threshold.action</name>
 <value>TRUNCATE</value>
 <description>DROP, TRUNCATE, REPLACE, or FAIL</description>
 </property>

 <property>
 <name>mycsv.data.field.length.threshold</name>
 <value>10000</value>
 </property>

 <property>
 <name>mycsv.data.threshold.action</name>
 <value>TRUNCATE</value>
 <description>DROP, TRUNCATE, REPLACE, or FAIL</description>
 </property>

 <property>
 <name>mycsv.data.category.field.aliases</name>
 <value>FILETYPE:FILE_TYPE</value>
 <description>These are the fields aliases (delimited by first separator).
 </description>
 </property>

 <property>
 <name>mycsv.data.category.index.blacklist</name>
 <value>FIELDNAME1_WE_DONT_WANT_INDEXED</value>
 <description>These are the fields to *not* index</description>
 </property>

 <property>
 <name>mycsv.data.category.index.reverse.blacklist</name>
 <value>FIELDNAME1_WE_DONT_WANT_INDEXED,SECURITY_MARKING,EVENT_DATE,PROCESSING_DATE</value>
 <description>These are the fields to *not* reverse index. Contains the same fields from *.index.blacklist and date, time, numeric, or hash-based fields</description>
 </property>

 <property>
 <name>mycsv.data.category.index.tokenize.blacklist</name>
 <value>FIELDNAME1_WE_DONT_WANT_INDEXED,FIELDNAME1_NOT_TO_TOKENIZE,UUID,PARENT_UUID,SECURITY_MARKING,EVENT_DATE,PROCESSING_DATE,EVENT_ID</value>
 <description>These are the fields to *not* tokenize and index.</description>
 </property>

 <property>
 <name>mycsv.data.category.index.reverse.tokenize.blacklist</name>
 <value>FIELDNAME1_WE_DONT_WANT_INDEXED,FIELDNAME1_NOT_TO_TOKENIZE,UUID,PARENT_UUID,SECURITY_MARKING,EVENT_DATE,PROCESSING_DATE,EVENT_ID</value>
 <description>These are the fields to *not* tokenize and index. Contains the same fields from *.index.blacklist and date, time, numeric, or hash-based fields</description>
 </property>

 <property>
 <name>mycsv.stopword.list.file</name>
 <value></value>
 <description>The stopword file to use. Default stop words will be used if none specified here</description>
 </property>

 <property>
 <name>mycsv.data.category.token.fieldname.designator.enabled</name>
 <value>false</value>
 <description>whether token fields should be named differently than their source fields</description>
 </property>

 <property>
 <name>mycsv.verbose.shard.counters</name>
 <value>false</value>
 <description>In the MapReduce ingest jobs, whether or not to increment
 counters for each "target" shard data will be loaded to. Warning:
 this is rather expensive and should not be 'true' for production
 </description>
 </property>

 <property>
 <name>mycsv.verbose.term.size.counters</name>
 <value>false</value>
 <description>In the MapReduce ingest jobs, whether or not to increment
 counters for the various term sizes. Warning: this is rather
 expensive and should not be 'true' for production
 </description>
 </property>

 <property>
 <name>mycsv.verbose.term.index.counters</name>
 <value>false</value>
 <description>In the MapReduce ingest jobs, whether or not to increment
 counters for the various term types. Warning: this is rather
 expensive and should not be 'true' for production
 </description>
 </property>

 <property>
 <name>mycsv.tokenizer.time.error.threshold.msec</name>
 <value>300000</value>
 <description>If a single document's tokenization exceeds this
 threshold, an exception will be thrown, causing the document to be
 placed in the processingErrors table (5*60*1000 = 300000 msec, 5
 minutes, which keeps it under the 10 minute hadoop job timeout
 threshold)
 </description>
 </property>

 <property>
 <name>mycsv.tokenizer.time.warn.threshold.msec</name>
 <value>150000</value>
 <description>If a single document's tokenization exceeds this threshold, a warning will be logged (2.5*60*1000 = 150000 msec, 2.5 minutes)</description>
 </property>

 <property>
 <name>mycsv.tokenizer.time.thresholds.msec</name>
 <value>1000,5000,10000,30000,60000,150000,300000</value>
 <description>time thresholds for tokenizer time histogram</description>
 </property>

 <property>
 <name>mycsv.tokenizer.time.threshold.names</name>
 <value>1s,5s,10s,30s,1m,2.5m,5m</value>
 <description>display values for time thresholds for tokenizer time histogram</description>
 </property>

 <property>
 <name>mycsv.verbose.term.index.counters</name>
 <value>true</value>
 <description></description>
 </property>

 <property>
 <name>mycsv.verbose.term.size.counters</name>
 <value>true</value>
 <description></description>
 </property>

 <property>
 <name>mycsv.data.default.type.class</name>
 <value>datawave.data.type.LcNoDiacriticsType</value>
 <description>Default type</description>
 </property>

 <property>
 <name>mycsv.data.default.type.class</name>
 <value>datawave.data.type.LcNoDiacriticsType</value>
 <description>Default type</description>
 </property>

 <property>
 <name>mycsv.data.category.date</name>
 <value>EVENT_DATE</value>
 </property>

 <property>
 <name>mycsv.data.category.date.formats</name>
 <value>yyyy-MM-dd'T'HH:mm:ss'Z',yyyy-MM-dd HH:mm:ss</value>
 </property>

 <property>
 <name>mycsv.data.id.parser.EVENT_ID.1</name>
 <value>datawave.ingest.metadata.id.DateIdParser("....\\.[a-zA-Z]([0-9]{7}).*", "yyyyDDD")</value>
 </property>

 <property>
 <name>mycsv.data.type.field.name</name>
 <value>DATA_TYPE</value>
 <description>where to look for a data type override value</description>
 </property>

 <property>
 <name>mycsv.event.data.type.keys</name>
 <value>K1,K2</value>
 <description>DATA_TYPE matching value</description>
 </property>

 <property>
 <name>mycsv.event.data.type.values</name>
 <value>V1,V2</value>
 <description>Event type value (compare to position in mycsv.event.data.type.keys)</description>
 </property>
 </configuration>
**/