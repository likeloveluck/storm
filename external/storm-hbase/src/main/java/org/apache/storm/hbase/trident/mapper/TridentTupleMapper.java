package org.apache.storm.hbase.trident.mapper;

import storm.trident.tuple.TridentTuple;

import java.io.Serializable;

/**
 * Created by judasheng on 14-12-11.
 */
public interface TridentTupleMapper extends Serializable {
    public String getKeyFromTridentTuple(TridentTuple tuple);
    public Object getValueFromTridentTuple(TridentTuple tuple);
}
