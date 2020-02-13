package org.apache.nifi.accumulo.processors;

import com.google.common.base.Splitter;
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
import datawave.ingest.mapreduce.job.metrics.MetricsConfiguration;
import datawave.ingest.test.StandaloneStatusReporter;
import datawave.marking.MarkingFunctions;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.nifi.accumulo.controllerservices.BaseAccumuloService;
import org.apache.nifi.accumulo.data.ContentRecordHandler;
import org.apache.nifi.accumulo.data.RecordContainer;
import org.apache.nifi.accumulo.data.RecordIngestHelper;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
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

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"hadoop", "accumulo", "put", "record"})
public class AddSplits extends DatawaveAccumuloIngest {

    protected static final PropertyDescriptor SPLIT_PREFIX = new PropertyDescriptor.Builder()
            .name("split-prefix")
            .displayName("Split prefix")
            .description("Split prefix")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    protected static final PropertyDescriptor NUM_SHARD = new PropertyDescriptor.Builder()
            .name("Num shards")
            .description("Number of shards. if 0 _{0-Num Shards} won't be present")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();



    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ACCUMULO_CONNECTOR_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(NUM_SHARD);
        properties.add(SPLIT_PREFIX);
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

    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        int shards =  processContext.getProperty(NUM_SHARD).asInteger();
        accumuloConnectorService = processContext.getProperty(ACCUMULO_CONNECTOR_SERVICE).asControllerService(BaseAccumuloService.class);
        final String table = processContext.getProperty(TABLE_NAME).getValue();
        final String prefix = processContext.getProperty(SPLIT_PREFIX).evaluateAttributeExpressions().getValue();

        SortedSet<Text> splits = new TreeSet<>();
        if (shards > 0){
            IntStream.range(0,shards).forEach(x ->{
                splits.add(new Text(prefix + "_" + x));
            });
        }
        else{
            splits.add(new Text(prefix));
        }

        try {
            accumuloConnectorService.getClient().tableOperations().addSplits(table,splits);
        } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
            throw new ProcessException(e);
        }

    }
}

