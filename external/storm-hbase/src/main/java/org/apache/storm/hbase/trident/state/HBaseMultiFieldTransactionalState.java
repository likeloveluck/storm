package org.apache.storm.hbase.trident.state;

import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.FailedException;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.common.HBaseClient;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by judasheng on 14-12-16.
 */
public class HBaseMultiFieldTransactionalState implements State {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseMultiFieldTransactionalState.class);

    private Options options;
    private HBaseClient hBaseClient;
    private Map map;
    private int numPartitions;
    private int partitionIndex;

    //used for transactional operation
    private Long curTxValue;

    protected HBaseMultiFieldTransactionalState(Map map, int partitionIndex, int numPartitions, final Options options) {
        this.options = options;
        this.map = map;
        this.partitionIndex = partitionIndex;
        this.numPartitions = numPartitions;

        final Configuration hbConfig = HBaseConfiguration.create();
        Map<String, Object> conf = (Map<String, Object>)map.get(options.configKey);
        if(conf == null){
            LOG.info("HBase configuration not found using key '" + options.configKey + "'");
            LOG.info("Using HBase config from first hbase-site.xml found on classpath.");
        } else {
            if (conf.get("hbase.rootdir") == null) {
                LOG.warn("No 'hbase.rootdir' value found in configuration! Using HBase defaults.");
            }
            for (String key : conf.keySet()) {
                hbConfig.set(key, String.valueOf(map.get(key)));
            }
        }

        this.hBaseClient = new HBaseClient(conf, hbConfig, options.tableName);
    }

    public static class Options implements Serializable {
        private Durability durability = Durability.SKIP_WAL;
        private String configKey;

        private TridentHBaseMapper mapper;
        private String tableName;
        private byte[] txColumnFamily;
        private byte[] txQualifier;
        private HBaseProjectionCriteria projectionCriteria = new HBaseProjectionCriteria();

        public Options withDurability(Durability durability) {
            this.durability = durability;
            return this;
        }

        public Options withColumn(String columnFamily, String qualifier) {
            projectionCriteria.addColumn(
                    new HBaseProjectionCriteria.ColumnMetaData(columnFamily, qualifier));
            return this;
        }

        public Options withTxColumn(String columnFamily, String qualifier) {
            this.txColumnFamily = columnFamily.getBytes();
            this.txQualifier = qualifier.getBytes();
            projectionCriteria.addColumn(
                    new HBaseProjectionCriteria.ColumnMetaData(columnFamily, qualifier));
            return this;
        }

        public Options withConfigKey(String configKey) {
            this.configKey = configKey;
            return this;
        }

        public Options withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Options withMapper(TridentHBaseMapper mapper) {
            this.mapper = mapper;
            return this;
        }
    }

    public static class Factory implements StateFactory {
        private Options options;

        public Factory(Options options) {
            this.options = options;
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            HBaseMultiFieldTransactionalState state = new HBaseMultiFieldTransactionalState(
                                                                conf, partitionIndex, numPartitions, options);
            return state;
        }
    }

    @Override
    public void beginCommit(Long txid) {
        curTxValue = txid;
    }

    @Override
    public void commit(Long txid) {
        curTxValue = null;
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
        List<byte[]> keys = Lists.newArrayList();
        List<ColumnList> valueList = Lists.newArrayList();
        for (TridentTuple tuple : tuples) {
            byte[] rowKey = options.mapper.rowKey(tuple);
            ColumnList cols = options.mapper.columns(tuple);
            keys.add(rowKey);
            valueList.add(cols);
        }

        List<HBaseMultiFieldTransactionalValue> curValues = multiGet(keys);
        List<byte[]> newKeys = Lists.newArrayList();
        List<HBaseMultiFieldTransactionalValue> newValues = Lists.newArrayList();
        for (int i = 0; i < curValues.size(); i++) {
            boolean changed = false;
            HBaseMultiFieldTransactionalValue curValue = curValues.get(i);
            HBaseMultiFieldTransactionalValue newValue = null;
            if (curValue == null) {
                newValue = new HBaseMultiFieldTransactionalValue(options.txColumnFamily, options.txQualifier,
                                                        String.valueOf(curTxValue).getBytes(), valueList.get(i));
                changed = true;
            } else {
                if (curTxValue != null && curTxValue.equals(curValue.getLongTxValue())) {
                    newValue = curValue;
                } else {
                    newValue = new HBaseMultiFieldTransactionalValue(options.txColumnFamily, options.txQualifier,
                            String.valueOf(curTxValue).getBytes(), valueList.get(i));
                    changed = true;
                }
            }
            if(changed) {
                newKeys.add(keys.get(i));
                newValues.add(newValue);
            }
        }
        if ( !newKeys.isEmpty() ) {
            multiPut(newKeys, newValues);
        }
    }

    public List<HBaseMultiFieldTransactionalValue> batchRetrieve(List<TridentTuple> tuples) {
        List<byte[]> keys = Lists.newArrayList();
        List<ColumnList> rows = Lists.newArrayList();
        for (TridentTuple tuple : tuples) {
            byte[] rowKey = options.mapper.rowKey(tuple);
            ColumnList cols = options.mapper.columns(tuple);
            keys.add(rowKey);
            rows.add(cols);
        }

        return multiGet(keys);
    }

    public void multiPut(List<byte[]> keys, List<HBaseMultiFieldTransactionalValue> values) {
        List<Mutation> mutations = Lists.newArrayList();

        for (int i = 0; i < keys.size(); i++) {
            byte[] key = keys.get(i);
            HBaseMultiFieldTransactionalValue transactionalValue = values.get(i);
            mutations.addAll(hBaseClient.constructMutationReq(key, transactionalValue.getAllColumns(), options.durability));
        }

        try {
            hBaseClient.batchMutate(mutations);
        } catch (Exception e) {
            LOG.warn("Batch write failed but some requests might have succeeded. Triggering replay.", e);
            throw new FailedException(e);
        }
    }

    private List<HBaseMultiFieldTransactionalValue> multiGet(List<byte[]> keys) {
        List<HBaseMultiFieldTransactionalValue> batchRetrieveResult = Lists.newArrayList();

        List<Get> gets = Lists.newArrayList();
        for (byte[] key : keys) {
            gets.add(hBaseClient.constructGetRequests(key, options.projectionCriteria));
        }

        try {
            Result[] results = hBaseClient.batchGet(gets);
            for(int i = 0; i < results.length; i++) {
                Result result = results[i];
                if ( !result.isEmpty() ) {
                    HBaseMultiFieldTransactionalValue transactionalValue = new HBaseMultiFieldTransactionalValue();
                    byte[] txValue = result.getValue(options.txColumnFamily, options.txQualifier);
                    transactionalValue.setTxColumn(options.txColumnFamily, options.txQualifier, txValue);
                    for (HBaseProjectionCriteria.ColumnMetaData column : options.projectionCriteria.getColumns()) {
                        byte[] value = result.getValue(column.getColumnFamily(), column.getQualifier());
                        transactionalValue.addColumn(column.getColumnFamily(), column.getQualifier(), value);
                    }
                    batchRetrieveResult.add(transactionalValue);
                } else {
                    batchRetrieveResult.add(null);
                }
            }
        } catch (Exception e) {
            LOG.warn("Batch get operation failed. Triggering replay.", e);
            throw new FailedException(e);
        }
        return batchRetrieveResult;
    }
}
