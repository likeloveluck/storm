package org.apache.storm.hbase.trident.state;

import storm.trident.state.ValueUpdater;

/**
 * Created by judasheng on 14-12-11.
 */
public class HBaseMapStateValueUpdater implements ValueUpdater {
    private Object org;

    public HBaseMapStateValueUpdater(Object org) {
        this.org = org;
    }

    @Override
    public Object update(Object stored) {
        return org;
    }
}
