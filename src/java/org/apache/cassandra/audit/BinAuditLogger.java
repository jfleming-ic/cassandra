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
package org.apache.cassandra.audit;

import java.nio.file.Paths;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.log.AbstractBinLogger;
import org.apache.cassandra.utils.binlog.BinLog;

public class BinAuditLogger extends AbstractBinLogger<AuditLogEntry, AuditLogOptions> implements IAuditLogger
{
    public static final long CURRENT_VERSION = 0;
    public static final String AUDITLOG_TYPE = "audit";
    public static final String AUDITLOG_MESSAGE = "message";
    protected static final Logger logger = LoggerFactory.getLogger(BinAuditLogger.class);

    public BinAuditLogger(AuditLogOptions options)
    {
        super(options);
        this.binLog = new BinLog.Builder(options).path(Paths.get(options.audit_logs_dir)).build(false);
    }

    @Override
    public void log(AuditLogEntry logEntry)
    {
        BinLog binLog = this.binLog;
        if (binLog == null || logEntry == null)
        {
            return;
        }
        binLog.logRecord(new Message(logEntry.getLogString()));
    }

    @VisibleForTesting
    public static class Message extends AbstractMessage
    {
        public Message(String message)
        {
            super(message);
        }

        @Override
        protected long version()
        {
            return CURRENT_VERSION;
        }

        @Override
        protected String type()
        {
            return AUDITLOG_TYPE;
        }

        @Override
        public void writeMarshallablePayload(WireOut wire)
        {
            wire.write(AUDITLOG_MESSAGE).text(message);
        }
    }
}
