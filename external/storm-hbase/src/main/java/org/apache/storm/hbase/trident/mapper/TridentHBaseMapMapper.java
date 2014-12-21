package org.apache.storm.hbase.trident.mapper;

import java.io.Serializable;
import java.util.List;

/**
 * Created by judasheng on 14-12-19.
 */
public interface TridentHBaseMapMapper extends Serializable {
    /**
     * Given a tuple's grouped key list, return the HBase rowkey.
     *
     * @param keys
     * @return
     */
    public byte[] rowKey(List<Object> keys);

    /**
     * Given a tuple's grouped key list, return the HBase qualifier.
     *
     * @param keys
     * @return
     */
    public String qualifier(List<Object> keys);
}
