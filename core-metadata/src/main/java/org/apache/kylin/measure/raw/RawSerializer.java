/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.measure.raw;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class RawSerializer extends DataTypeSerializer<List<ByteArray>> {

    //one dictionary id value need 2 bytes, this buffer can contain ~ 512 * 1024 values
    //FIXME to config this and RowConstants.ROWVALUE_BUFFER_SIZE in properties file
    public static final int RAW_BUFFER_SIZE = 1024 * 1024;//1M

    public RawSerializer(DataType dataType) {
    }

    @Override
    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        int len = 0;
        if (in.hasRemaining()) {
            int size = BytesUtil.readVInt(in);
            int bytes = BytesUtil.readVInt(in);
            len = in.position() - mark + bytes;
        }
        in.position(mark);
        return len;
    }

    @Override
    public int maxLength() {
        return RAW_BUFFER_SIZE;
    }

    @Override
    public int getStorageBytesEstimate() {
        return RAW_BUFFER_SIZE;
    }

    @Override
    public void serialize(List<ByteArray> value, ByteBuffer out) {
        if (value != null) {
            int bytes = 0;
            for (ByteArray array : value) {
                //in BytesUtil.writeByteArray, writeVInt only need 1 byte,
                //because dictionary id length is less than 127
                //so one value will need 2 bytes
                bytes += (array.length() + 1);
            }
            //size and bytes value will need <=8 bytes
            BytesUtil.writeVInt(value.size(), out);
            BytesUtil.writeVInt(bytes, out);
            if (bytes > out.remaining()) {
                throw new RuntimeException("BufferOverflow! Please use one higher cardinality column for dimension column when build RAW cube!");
            }
            for (ByteArray array : value) {
                BytesUtil.writeByteArray(array.array(), out);
            }
        }
    }

    @Override
    public List<ByteArray> deserialize(ByteBuffer in) {
        List<ByteArray> value = null;
        if (in.hasRemaining()) {
            int size = BytesUtil.readVInt(in);
            int bytes = BytesUtil.readVInt(in);
            value = new ArrayList<ByteArray>(size);
            for (int i = 0; i < size; i++) {
                value.add(new ByteArray(BytesUtil.readByteArray(in)));
            }
        }
        return value;
    }

}
