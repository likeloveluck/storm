package org.apache.storm.hbase.trident.state;

import backtype.storm.tuple.Values;
import org.apache.storm.hbase.trident.mapper.TridentTupleMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by judasheng on 14-12-11.
 */
public class HBaseMapStateUpdater implements StateUpdater<MapState> {
    private static Logger logger = LoggerFactory.getLogger(HBaseMapStateUpdater.class);

    TridentTupleMapper tupleMapper;

    public HBaseMapStateUpdater(TridentTupleMapper tupleExtractor) {
        this.tupleMapper = tupleExtractor;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void updateState(MapState state, List<TridentTuple> tuples, TridentCollector collector) {
        List<List<Object>> keys = new ArrayList<List<Object>>();
        List<ValueUpdater> updaters = new ArrayList<ValueUpdater>();
        for (TridentTuple tuple : tuples) {
            String key = tupleMapper.getKeyFromTridentTuple(tuple);
            Object value = tupleMapper.getValueFromTridentTuple(tuple);
            keys.add(new Values(key));
            updaters.add(new HBaseMapStateValueUpdater(value));
            logger.info("HBaseMapStateUpdater update key[" + key + "] value[" + value + "]");
        }
        state.multiUpdate(keys, updaters);
    }
}
