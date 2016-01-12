package org.apache.kylin.measure.raw;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RawSerializerTest {
    static {
        DataType.register("raw");
    }

    private RawSerializer rawSerializer = new RawSerializer(DataType.getType("raw"));

    @Test
    public void testNormal() {
        List<ByteArray> input = getValueList(1024);
        List<ByteArray> output = doSAndD(input);
        assertEquals(input, output);
    }

    @Test
    public void testNull() {
        List<ByteArray> output = doSAndD(null);
        assertEquals(output.size(), 0);
        List<ByteArray> input = new ArrayList<ByteArray>();
        output = doSAndD(input);
        assertEquals(input, output);
    }

    @Test(expected = RuntimeException.class)
    public void testOverflow() {
        List<ByteArray> input = getValueList(512 * 1024);
        doSAndD(input);
    }

    private List<ByteArray> doSAndD(List<ByteArray> input) {
        ByteBuffer out = ByteBuffer.allocate(rawSerializer.maxLength());
        out.mark();
        rawSerializer.serialize(input, out);
        out.reset();
        return rawSerializer.deserialize(out);
    }

    private List<ByteArray> getValueList(int size) {
        if (size == -1) {
            return null;
        }
        List<ByteArray> valueList = new ArrayList<ByteArray>(size);
        for (Integer i = 0; i < size; i++) {
            ByteArray key = new ByteArray(1);
            BytesUtil.writeUnsigned(i, key.array(), 0, key.length());
            valueList.add(key);
        }
        return valueList;
    }

}
