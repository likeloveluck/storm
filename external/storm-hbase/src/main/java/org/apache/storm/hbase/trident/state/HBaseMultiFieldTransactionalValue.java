package org.apache.storm.hbase.trident.state;

import org.apache.storm.hbase.common.ColumnList;

/**
 * Created by judasheng on 14-12-16.
 */
public class HBaseMultiFieldTransactionalValue {

    private byte[] txColumnFamily;
    private byte[] txQualifier;
    private byte[] txValue;

    private ColumnList columns;

    public HBaseMultiFieldTransactionalValue() {
        this.columns = new ColumnList();
    }

    public HBaseMultiFieldTransactionalValue(byte[] family, byte[] qualifier, byte[] value, ColumnList columns) {
        this.txColumnFamily = family;
        this.txQualifier = qualifier;
        this.txValue = value;
        this.columns = columns;
    }

    public void setTxColumn(byte[] family, byte[] qualifier, byte[] value) {
        this.txColumnFamily = family;
        this.txQualifier = qualifier;
        this.txValue = value;
    }

    public void setColumns(ColumnList columns) {
        this.columns = columns;
    }

    public void addColumn(byte[] family, byte[] qualifier, byte[] value) {
        this.columns.addColumn(family, qualifier, value);
    }

    public byte[] getTxColumnFamily() {
        return txColumnFamily;
    }

    public byte[] getTxQualifier() {
        return txQualifier;
    }

    public byte[] getTxValue() {
        return txValue;
    }

    public Long getLongTxValue() {
        return Long.valueOf(String.valueOf(txValue));
    }

    public ColumnList getColumns() {
        return columns;
    }

    public ColumnList getAllColumns() {
        ColumnList allColumns = new ColumnList();
        allColumns.addColumn(txColumnFamily, txQualifier, txValue);
        for (ColumnList.Column col : columns.getColumns()) {
            allColumns.addColumn(col.getFamily(), col.getQualifier(), col.getValue());
        }
        for (ColumnList.Counter counter : columns.getCounters()) {
            allColumns.addCounter(counter.getFamily(), counter.getQualifier(), counter.getIncrement());
        }
        return allColumns;
    }
}
