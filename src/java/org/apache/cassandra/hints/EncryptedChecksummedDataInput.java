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
package org.apache.cassandra.hints;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContext.ChannelProxyReadChannel;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.utils.Throwables;

public class EncryptedChecksummedDataInput extends ChecksummedDataInput
{
    private final EncryptionContext encryptionContext;
    private final ChannelProxyReadChannel readChannel;
    private long sourcePosition;

    protected EncryptedChecksummedDataInput(ChannelProxy channel, EncryptionContext encryptionContext, long filePosition)
    {
        super(channel);
        assert encryptionContext != null;
        this.encryptionContext = encryptionContext;
        readChannel = new EncryptionContext.ChannelProxyReadChannel(channel, filePosition);
        this.sourcePosition = filePosition;
    }

    /**
     * Since an entire block of compressed data is read off of disk, not just a hint at a time,
     * we don't report EOF until the decompressed data has also been read completely
     */
    public boolean isEOF()
    {
        return readChannel.getCurrentPosition() == channel.size() && buffer.remaining() == 0;
    }

    public long getSourcePosition()
    {
        return sourcePosition;
    }

    static class Position extends ChecksummedDataInput.Position
    {
        final long bufferStart;
        final int bufferPosition;

        public Position(long sourcePosition, long bufferStart, int bufferPosition)
        {
            super(sourcePosition);
            this.bufferStart = bufferStart;
            this.bufferPosition = bufferPosition;
        }

        @Override
        public long subtract(InputPosition o)
        {
            Position other = (Position) o;
            return bufferStart - other.bufferStart + bufferPosition - other.bufferPosition;
        }
    }

    public InputPosition getSeekPosition()
    {
        return new Position(sourcePosition, bufferOffset, buffer.position());
    }

    public void seek(InputPosition p)
    {
        Position pos = (Position) p;
        bufferOffset = pos.bufferStart;
        readChannel.setPosition(pos.sourcePosition);
        buffer.position(0).limit(0);
        resetCrc();
        reBuffer();
        buffer.position(pos.bufferPosition);
        assert sourcePosition == pos.sourcePosition;
        assert bufferOffset == pos.bufferStart;
        assert buffer.position() == pos.bufferPosition;
    }

    @Override
    protected void readBuffer()
    {
        this.sourcePosition = readChannel.getCurrentPosition();
        if (isEOF())
            return;

        try
        {
            buffer = encryptionContext.decrypt(readChannel, buffer, true);
            buffer.flip();
        }
        catch (IOException ioe)
        {
            throw new FSReadError(ioe, getPath());
        }
    }

    @SuppressWarnings("resource")
    public static ChecksummedDataInput upgradeInput(ChecksummedDataInput input, EncryptionContext encryptionContext)
    {
        long position = input.getPosition();
        input.close();

        ChannelProxy channel = new ChannelProxy(input.getPath());
        try
        {
            return new EncryptedChecksummedDataInput(channel, encryptionContext, position);
        }
        catch (Throwable t)
        {
            throw Throwables.cleaned(channel.close(t));
        }
    }

    @VisibleForTesting
    public EncryptionContext getEncryptionContext()
    {
        return encryptionContext;
    }
}
