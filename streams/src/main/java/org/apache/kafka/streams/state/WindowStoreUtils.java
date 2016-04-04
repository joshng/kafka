/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.nio.ByteBuffer;

public class WindowStoreUtils {

    private static final int SEQNUM_SIZE = 4;
    private static final int TIMESTAMP_SIZE = 8;

    /** Inner byte array serde used for segments */
    public static final Serde<byte[]> BYTE_ARRAY_SERDE = Serdes.ByteArray();
    public static final Serde<ByteBuffer> BYTE_BUFFER_SERDE = Serdes.ByteBuffer();

    /** Inner state serde used for segments */
    public static final StateSerdes<ByteBuffer, byte[]> INNER_SERDES = new StateSerdes<>("", BYTE_BUFFER_SERDE, BYTE_ARRAY_SERDE);

    @SuppressWarnings("unchecked")
    public static final KeyValueIterator<ByteBuffer, byte[]>[] NO_ITERATORS = (KeyValueIterator<ByteBuffer, byte[]>[]) new KeyValueIterator[0];

    public static <K> ByteBuffer toBinaryKey(K key, final long timestamp, final int seqnum, StateSerdes<K, ?> serdes) {
        byte[] serializedKey = serdes.rawKey(key);

        ByteBuffer buf = ByteBuffer.allocate(serializedKey.length + TIMESTAMP_SIZE + SEQNUM_SIZE);
        buf.put(serializedKey);
        buf.putLong(timestamp);
        buf.putInt(seqnum);

        return buf;
    }

    public static <K> K keyFromBinaryKey(ByteBuffer binaryKey, StateSerdes<K, ?> serdes) {
        byte[] bytes = new byte[binaryKey.remaining() - TIMESTAMP_SIZE - SEQNUM_SIZE];

        binaryKey.duplicate().get(bytes);

        return serdes.keyFrom(bytes);
    }

    public static long timestampFromBinaryKey(ByteBuffer binaryKey) {
        return binaryKey.getLong(binaryKey.remaining() - TIMESTAMP_SIZE - SEQNUM_SIZE);
    }
}
