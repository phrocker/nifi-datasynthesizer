package org.apache.nifi.accumulo.data;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.serialization.record.RecordFieldType;

import datawave.data.type.Type;
import java.util.Map;
import java.util.HashMap;

import datawave.data.type.DateType;
import datawave.data.type.GeoLatType;
import datawave.data.type.GeoLonType;
import datawave.data.type.NumberType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaNormalizers {

  private Map<java.lang.String,Map<String,Type<?>>> typeMap;
  private static final Logger log = LoggerFactory.getLogger(SchemaNormalizers.class);
  
    private SchemaNormalizers() {
        typeMap = new HashMap<>();
    }

    static SchemaNormalizers normalizer = null;

    public static SchemaNormalizers getNormalizers(){
        if (null == normalizer){
            
        synchronized (SchemaNormalizers.class){
            if (null != normalizer){
                return normalizer;
            }
            normalizer = new SchemaNormalizers( );
        }
    }
        return normalizer;
    }


public synchronized Type<?> getType(String type, String fieldName) {
    Map<String,Type<?>> om = typeMap.get(type);
    if (null == om){
        om = new HashMap<>();
        typeMap.put(type,om);
        return null;
    }
    Type<?> t = om.get(fieldName);
    return t;
  }

  public synchronized void setType(String type, String fieldName, Type<?> baseType) {
    Map<String,Type<?>> om = typeMap.get(type);
    if (null == om){
        om = new HashMap<>();
        typeMap.put(type,om);
    }
    om.put(fieldName,baseType);
  }

  public synchronized void setType(String type, String fieldName, RecordFieldType schemaType) {
    Type<?> baseType = null;

    switch(schemaType){
        case SHORT:
        case INT:
        case LONG:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
            if (fieldName.toLowerCase().contains("lati")){
                baseType = datawave.data.type.Type.Factory.createType(GeoLatType.class.getCanonicalName()); 
            }else if (fieldName.toLowerCase().contains("longi")){
                baseType = datawave.data.type.Type.Factory.createType(GeoLonType.class.getCanonicalName()); 
            }
            else{
                baseType = datawave.data.type.Type.Factory.createType(NumberType.class.getCanonicalName()); 
            }
            break;
        case DATE:
        case TIME:
        case TIMESTAMP:
            baseType = datawave.data.type.Type.Factory.createType(DateType.class.getCanonicalName()); 
            break;
        default:
            baseType=null;
    };
    if (null == baseType){
        if (fieldName.toLowerCase().contains("lati")){
            baseType = datawave.data.type.Type.Factory.createType(GeoLatType.class.getCanonicalName()); 
        }else if (fieldName.toLowerCase().contains("longi")){
            baseType = datawave.data.type.Type.Factory.createType(GeoLonType.class.getCanonicalName()); 
        }else if (fieldName.toLowerCase().contains("date")){
            baseType = datawave.data.type.Type.Factory.createType(DateType.class.getCanonicalName()); 
        }
        else{
            baseType = null;
        }
        if (null == baseType){
            log.info("baseType is null ({} for {})",schemaType,fieldName);
        }else{
            log.info("{}} is {}",fieldName,baseType.getClass().getCanonicalName());
        }
    }
    else{
        log.info("{}} is {}",fieldName,baseType.getClass().getCanonicalName());
    }
    
    Map<String,Type<?>> om = typeMap.get(type);
    if (null == om){
        om = new HashMap<>();
        typeMap.put(type,om);
    }
    om.put(fieldName,baseType);
    om.put(fieldName.toUpperCase(),baseType);
  }
     
}