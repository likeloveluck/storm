package org.apache.storm.hbase.trident.state;

import storm.trident.tuple.TridentTuple;

/**
 * Created by judasheng on 14-12-11.
 */
public interface HBaseMapStateTupleMapper {
    public String getKeyFromTridentTuple(TridentTuple tuple);
    public Object getValueFromTridentTuple(TridentTuple tuple);
}
