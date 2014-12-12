package org.apache.storm.hbase.trident.mapper;

import storm.trident.tuple.TridentTuple;

/**
 * Created by judasheng on 14-12-11.
 */
public interface TridentTupleMapper {
    public String getKeyFromTridentTuple(TridentTuple tuple);
    public Object getValueFromTridentTuple(TridentTuple tuple);
}
