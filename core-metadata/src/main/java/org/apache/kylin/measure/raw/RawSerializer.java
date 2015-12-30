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


    public RawSerializer(DataType dataType) {
    }

    @Override
    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        int len = 0;
        if(in.hasRemaining()) {
            int size = in.getInt();
            int bytes = in.getInt();
            len = in.position() - mark + bytes;
        }
        in.position(mark);
        return len;
    }

    @Override
    public int maxLength() {
        return 1024 * 1024;
    }

    @Override
    public int getStorageBytesEstimate() {
        return 1024 * 1024;
    }

    @Override
    public void serialize(List<ByteArray> value, ByteBuffer out) {
        if(value != null) {
            int bytes = 0;
            for (ByteArray array : value) {
                bytes += (array.length() + 1);
            }
            if (bytes > out.remaining()) {
                throw new RuntimeException("BufferOverflow! Please use one higher cardinality column for dimension column when build RAW cube!");
            }
            out.putInt(value.size());
            out.putInt(bytes);
            for (ByteArray array : value) {
                BytesUtil.writeByteArray(array.array(), out);
            }
        }
    }

    @Override
    public List<ByteArray> deserialize(ByteBuffer in) {
        List<ByteArray> value = null;
        if(in.hasRemaining()) {
            int size = in.getInt();
            int bytes = in.getInt();
            value = new ArrayList<ByteArray>(size);
            for (int i = 0; i < size; i++) {
                value.add(new ByteArray(BytesUtil.readByteArray(in)));
            }
        }
        return value;
    }

}
