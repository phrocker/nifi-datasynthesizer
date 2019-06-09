package org.poma.accumulo.nifi.data;

import org.apache.nifi.serialization.record.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class KeySchema implements RecordSchema {
    private static final List<RecordField> KEY_FIELDS = new ArrayList<>();

    private static final List<DataType> DATA_TYPES = new ArrayList<>();

    private static final List<String> FIELD_NAMES = new ArrayList<>();

    static {
        KEY_FIELDS.add(new RecordField("row",RecordFieldType.STRING.getDataType(),false));
        KEY_FIELDS.add(new RecordField("columnFamily",RecordFieldType.STRING.getDataType(),true));
        KEY_FIELDS.add(new RecordField("columnQualifier",RecordFieldType.STRING.getDataType(),true));
        KEY_FIELDS.add(new RecordField("columnVisibililty",RecordFieldType.STRING.getDataType(),true));
        KEY_FIELDS.add(new RecordField("timestamp",RecordFieldType.LONG.getDataType(),true));
        DATA_TYPES.add(RecordFieldType.STRING.getDataType());
        DATA_TYPES.add(RecordFieldType.LONG.getDataType());

        FIELD_NAMES.addAll(KEY_FIELDS.stream().map( x-> { return x.getFieldName();}).collect(Collectors.toList()));
    }
    @Override
    public List<RecordField> getFields() {
        return KEY_FIELDS;
    }

    @Override
    public int getFieldCount() {
        return KEY_FIELDS.size();
    }

    @Override
    public RecordField getField(int i) {
        return KEY_FIELDS.get(i);
    }

    @Override
    public List<DataType> getDataTypes() {
        return DATA_TYPES;
    }

    @Override
    public List<String> getFieldNames() {
        return FIELD_NAMES;
    }

    @Override
    public Optional<DataType> getDataType(String s) {
        if (s.equalsIgnoreCase("timestamp")){
            return Optional.of( RecordFieldType.LONG.getDataType() );
        }
        else{
            if (FIELD_NAMES.stream().filter(x -> s.equalsIgnoreCase(s)).count() > 0){
                return  Optional.of(RecordFieldType.STRING.getDataType());
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<String> getSchemaText() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getSchemaFormat() {
        return Optional.empty();
    }

    @Override
    public Optional<RecordField> getField(final String s) {
        return KEY_FIELDS.stream().filter(x -> x.getFieldName().equalsIgnoreCase(s)).findFirst();
    }

    @Override
    public SchemaIdentifier getIdentifier() {
        return SchemaIdentifier.builder().name("AccumuloKeySchema").version(1).branch("nifi-accumulo").build();
    }

    @Override
    public Optional<String> getSchemaName() {
        return Optional.of("AccumuloKeySchema");
    }

    @Override
    public Optional<String> getSchemaNamespace() {
        return Optional.of("nifi-accumulo");
    }
}
