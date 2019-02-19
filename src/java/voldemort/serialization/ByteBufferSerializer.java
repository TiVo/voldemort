/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.serialization;


import java.nio.ByteBuffer;

/**
 * A Serializer used to serialize a ByteBuffer.  This is used to build read only stores from incoming data that is
 * already serialized by the user and transported in Avro as a ByteBuffer.
 */
public class ByteBufferSerializer implements Serializer<ByteBuffer> {

    public byte[] toBytes(ByteBuffer buffer) {
        return buffer.array();
    }

    public ByteBuffer toObject(byte[] bytes) {
        throw new UnsupportedOperationException();
    }

}
