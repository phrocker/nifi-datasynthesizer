package org.apache.nifi.accumulo.data;


import org.apache.hadoop.conf.Configuration;
import datawave.data.type.Type;
import java.util.List;
import java.util.ArrayList;
import datawave.data.type.LcNoDiacriticsType;
import datawave.ingest.data.config.ingest.EventFieldNormalizerHelper;

public class RecordSchemaNormalizer extends EventFieldNormalizerHelper {
      
    private Type<?> baseType;
   
    private SchemaNormalizers normalizers;
    
  
    public RecordSchemaNormalizer(Configuration arg0) {
        super(arg0);
        baseType = datawave.data.type.Type.Factory.createType(LcNoDiacriticsType.class.getCanonicalName());
        normalizers = SchemaNormalizers.getNormalizers();
    }

    static RecordSchemaNormalizer normalizer = null;

    public boolean isNormalizedField(String fieldName){
        return true;
    }

    public List<Type<?>> getDataTypes(String fieldName){
        ArrayList<Type<?>> arr = new ArrayList<>();
        arr.add( getType(fieldName) );
        return arr;
    }

    @Override
    public Type<?> getType(String fieldName) {
        Type<?> tp = normalizers.getType(getType().typeName(),fieldName);
        if (null == tp){
            return baseType;
        }
        else{
            return tp;
        }
    }

     
}