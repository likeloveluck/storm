package org.apache.storm.hbase.trident.mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created by judasheng on 14-12-26.
 */
public class SimpleTridentHBaseMapMapper implements TridentHBaseMapMapper {
    private String qualifier;

    public SimpleTridentHBaseMapMapper(String qualifier) {
        this.qualifier = qualifier;
    }

    @Override
    public byte[] rowKey(List<Object> keys) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            for (Object key : keys) {
                bos.write(String.valueOf(key).getBytes());
            }
            bos.close();
        } catch (IOException e){
            throw new RuntimeException("IOException creating HBase row key.", e);
        }
        return bos.toByteArray();
    }

    @Override
    public String qualifier(List<Object> keys) {
        return this.qualifier;
    }
}
