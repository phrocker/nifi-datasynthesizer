package org.apache.nifi.accumulo.data;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.config.RawRecordContainerImpl;

import java.util.*;

public class RecordContainer extends RawRecordContainerImpl {

    Multimap<String,String> map = HashMultimap.create();

    Set<String> indexedFields = new HashSet<>();

    long size = 0;

    public RecordContainer(){

    }

    public void setSize(long size){
        this.size=size;
    }

    @Override
    public long getRawDataSize(){
        return size;
    }

    public void setMap(final Multimap<String,String> map){
        this.map=map;
    }

    public void addIndexedFields(final Collection<String> fields){
        indexedFields.addAll(fields);
    }

    public boolean isIndexedField(final String field){
        return indexedFields.contains(field);
    }

    @Override
    public void clear(){
        super.clear();
        map = HashMultimap.create();
        indexedFields = new HashSet<>();
    }

    public Multimap<String, String> getMap() {
        return map;
    }
}
