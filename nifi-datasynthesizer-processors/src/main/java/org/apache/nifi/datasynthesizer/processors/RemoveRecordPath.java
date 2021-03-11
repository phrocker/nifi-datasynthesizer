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
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.serialization.record.RecordSchema;
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
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.record.path.validation.RecordPathPropertyNameValidator;
import org.apache.nifi.serialization.RecordSetWriter;
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
public class RemoveRecordPath extends AbstractProcessor {



    public RemoveRecordPath(){}
    protected static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();


    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    private RecordReaderFactory recordParserFactory;
    private RecordSetWriterFactory writerFactory;

    protected UIDBuilder<UID> uidBuilder = UID.builder();

    private volatile RecordPathCache recordPathCache;
    private volatile List<String> recordPaths;
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER_FACTORY);
        properties.add(RECORD_WRITER);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .description("Specifies the value to use to remove fields that match the RecordPath: " + propertyDescriptorName)
            .required(false)
            .dynamic(true)
            .expressionLanguageSupported(true)
            .addValidator(new RecordPathPropertyNameValidator())
            .build();
    }


    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
    .name("original")
    .description("Unmodified original record.")
    .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Record path was processed and fields removed iff they exist")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Record path(s) could not be evaluated. ")
            .build();


    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        rels.add(REL_ORIGINAL);
        return rels;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final boolean containsDynamic = validationContext.getProperties().keySet().stream()
            .anyMatch(property -> property.isDynamic());

        if (containsDynamic) {
            return Collections.emptyList();
        }

        return Collections.singleton(new ValidationResult.Builder()
            .subject("User-defined Properties")
            .valid(false)
            .explanation("At least one RecordPath must be specified")
            .build());
    }

    SetMultimap<String, String> recordPathFromEdges = HashMultimap.create();
    SetMultimap<String, String> recordPathToEdges =  HashMultimap.create();
    Map<String, Pattern> recordPathFromRegexes = new HashMap<>();
    Map<String, Pattern> recordPathToRegexes = new HashMap<>();

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ClassNotFoundException, IllegalAccessException, InstantiationException, TableExistsException, AccumuloSecurityException, AccumuloException {

        recordPathCache = new RecordPathCache(context.getProperties().size() * 2);

        final List<String> recordPaths = new ArrayList<>(context.getProperties().size() - 2);
        for (final PropertyDescriptor property : context.getProperties().keySet()) {
            if (property.isDynamic()) {
                recordPaths.add(property.getName());
            }
        }

        this.recordPaths = recordPaths;
        this.recordParserFactory = context.getProperty(RECORD_READER_FACTORY)
        .asControllerService(RecordReaderFactory.class);
        this.writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
    }


    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        final FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }

        try (final InputStream in = processSession.read(flowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(flowFile, in, getLogger())) {
            final FlowFile newFlowFile = processSession.write(processSession.create(), (inputStream, out) -> {
                Map<String, String> obj = new HashMap<>();
                try {
                    final RecordSchema writeSchema = writerFactory.getSchema(obj, reader.getSchema());
                    try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out)) {
                        writer.beginRecordSet();
                        Record record;
                        while ((record = reader.nextRecord()) != null) {
                        
                            for (final String recordPathText : recordPaths) {
                                final RecordPath recordPath = recordPathCache.getCompiled(recordPathText);
                                final RecordPathResult result = recordPath.evaluate(record);
                                result.getSelectedFields().forEach(field -> field.updateValue(null));
                                //result.getSelectedFields().forEach(field -> record.setValue(field.getField(),null ));
                            }

                            writer.write(record);
                        }
                    }
                } catch (Exception ex) {
                    processSession.transfer(flowFile,REL_FAILURE);
                    getLogger().error("Error while processing record path.", ex);
                    return;
                }
            });
            processSession.transfer(newFlowFile,REL_SUCCESS);
            processSession.transfer(flowFile,REL_ORIGINAL);

        } catch (Exception ex) {
            processSession.transfer(flowFile,REL_FAILURE);
            getLogger().error("Error while processing record path.", ex);
        }
     
        



    }
    
}

