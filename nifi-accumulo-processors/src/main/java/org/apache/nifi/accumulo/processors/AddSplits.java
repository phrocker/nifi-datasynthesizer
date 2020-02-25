package org.apache.nifi.accumulo.processors;

import org.apache.accumulo.core.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;
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
            getClient(processContext).tableOperations().addSplits(table,splits);
        } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
            throw new ProcessException(e);
        }

    }
}

