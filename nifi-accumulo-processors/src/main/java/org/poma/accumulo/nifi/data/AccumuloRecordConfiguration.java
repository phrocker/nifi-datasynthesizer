/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software

 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.poma.accumulo.nifi.data;

public class AccumuloRecordConfiguration {


    private String tableName;
    private String rowFieldName;
    private String columnFamily;
    private boolean qualifierInKey;
    private String timestampField;

    protected AccumuloRecordConfiguration(String tableName, String rowFieldName, String columnFamily, String timestampField, boolean qualifierInKey)
    {
        this.tableName=tableName;
        this.rowFieldName=rowFieldName;
        this.columnFamily=columnFamily;
        this.timestampField=timestampField;
        this.qualifierInKey=qualifierInKey;
    }

    public String getTableName(){
        return tableName;
    }

    public String getColumnFamily() {
        return columnFamily;
    }


    public String getTimestampField(){
        return timestampField;
    }

    public boolean getQualifierInKey(){
        return qualifierInKey;
    }


    public String getRowFIeld(){
        return rowFieldName;
    }

    public static class Builder{

        public static final Builder newBuilder(){
            return new Builder();
        }

        public Builder setRowField(final String rowFieldName){
            this.rowFieldName=rowFieldName;
            return this;
        }

        public Builder setTableName(final String tableName){
            this.tableName=tableName;
            return this;
        }



        public Builder setColumnFamily(final String columnFamily){
            this.columnFamily=columnFamily;
            return this;
        }

        public Builder setTimestampField(final String timestampField){
            this.timestampField=timestampField;
            return this;
        }

        public Builder setQualifierInKey(final boolean qualifierInKey){
            this.qualifierInKey=qualifierInKey;
            return this;
        }

        public AccumuloRecordConfiguration build(){
            return new AccumuloRecordConfiguration(tableName,rowFieldName,columnFamily,timestampField,qualifierInKey);
        }


        private String tableName;
        private String rowFieldName;
        private String columnFamily;
        private boolean qualifierInKey;
        private String timestampField;
    }
}
