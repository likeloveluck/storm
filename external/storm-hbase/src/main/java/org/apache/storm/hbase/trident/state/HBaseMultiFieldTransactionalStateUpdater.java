package org.apache.storm.hbase.trident.state;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

/**
 * Created by judasheng on 14-12-16.
 */
public class HBaseMultiFieldTransactionalStateUpdater implements StateUpdater<HBaseMultiFieldTransactionalState> {
    @Override
    public void updateState(HBaseMultiFieldTransactionalState state, List<TridentTuple> tuples, TridentCollector collector) {
        state.updateState(tuples, collector);
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
