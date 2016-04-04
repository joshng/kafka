/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.state.internals;

import java.nio.ByteBuffer;
import java.util.Comparator;

public final class LexicographicByteBufferComparator implements Comparator<ByteBuffer> {
    public static final LexicographicByteBufferComparator INSTANCE = new LexicographicByteBufferComparator();

    public static int compareBuffers(ByteBuffer left, ByteBuffer right) {
        if (left == right) {
            return 0;
        }
        return compare(left, left.position(), left.remaining(), right, right.position(), right.limit());
    }

    @Override
    public int compare(ByteBuffer left, ByteBuffer right) {
        return compareBuffers(left, right);
    }

    public static int compare(ByteBuffer a, int aPos, int aLen, ByteBuffer b, int bPos, int bLen) {
        int minLength = Math.min(aLen, bLen);
        int minWords = minLength / Long.BYTES;

        for (int i = 0; i < minWords; i++) {
            int offset = i * Long.BYTES;
            int result = compareUnsignedLongs(a.getLong(aPos + offset), b.getLong(bPos + offset));
            if (result != 0) {
                return result;
            }
        }

        // check the last (minLength % 8) bytes
        for (int i = minWords * Long.BYTES; i < minLength; i++) {
            int result = compareUnsignedBytes(a.get(aPos + i), b.get(bPos + i));
            if (result != 0) {
                return result;
            }
        }
        return aLen - bLen;
    }

    public static int compareUnsignedLongs(long a, long b) {
        return compareSignedLongs(flipLong(a), flipLong(b));
    }

    public static int compareSignedLongs(long a, long b) {
        return (a < b) ? -1 : ((a > b) ? 1 : 0);
    }

    public static int compareUnsignedBytes(byte a, byte b) {
        return (a & 0xff) - (b & 0xff);
    }

    private static long flipLong(long a) {
        return a ^ Long.MIN_VALUE;
    }
}
