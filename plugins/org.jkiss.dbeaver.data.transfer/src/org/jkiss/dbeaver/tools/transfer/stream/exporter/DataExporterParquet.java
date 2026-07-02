/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2026 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.tools.transfer.stream.exporter;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.data.DBDAttributeBinding;
import org.jkiss.dbeaver.model.data.DBDContent;
import org.jkiss.dbeaver.model.data.DBDContentStorage;
import org.jkiss.dbeaver.model.exec.DBCResultSet;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.tools.transfer.stream.IStreamDataExporterSite;
import org.jkiss.dbeaver.utils.ContentUtils;
import org.jkiss.utils.CommonUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataExporterParquet extends StreamExporterAbstract {

    private static final Log log = Log.getLog(DataExporterParquet.class);

    private static final String PROP_COMPRESSION = "compression";
    private static final String PROP_NULL_STRING = "nullString";

    private DBDAttributeBinding[] columns;
    private MessageType schema;
    private ParquetWriter<Object[]> writer;
    private String nullString;
    private CompressionCodecName compressionCodec;

    @Override
    public void init(IStreamDataExporterSite site) throws DBException {
        super.init(site);
        Map<String, Object> props = site.getProperties();
        nullString = CommonUtils.toString(props.get(PROP_NULL_STRING), "");
        String compressionName = CommonUtils.toString(props.get(PROP_COMPRESSION), "SNAPPY");
        try {
            compressionCodec = CompressionCodecName.fromConf(compressionName);
        } catch (Exception e) {
            compressionCodec = CompressionCodecName.SNAPPY;
        }
    }

    @Override
    public void exportHeader(DBCSession session) throws DBException, IOException {
        columns = getSite().getAttributes();
        if (columns == null || columns.length == 0) {
            throw new DBException("No columns to export");
        }
        schema = buildSchema();

        OutputStream outputStream = getOutputStream();
        OutputFile outputFile = new OutputStreamOutputFile(outputStream);

        ObjectArrayWriteSupport writeSupport = new ObjectArrayWriteSupport(schema, columns, nullString);

        Configuration conf = new Configuration(false);

        writer = ParquetWriter
            .<Object[]>builder(outputFile, writeSupport)
            .withConf(conf)
            .withCompressionCodec(compressionCodec)
            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
            .build();
    }

    @Override
    public void exportRow(DBCSession session, DBCResultSet resultSet, Object[] row) throws DBException, IOException {
        Object[] converted = new Object[row.length];
        for (int i = 0; i < row.length; i++) {
            converted[i] = convertValue(session, row[i]);
        }
        writer.write(converted);
    }

    @Override
    public void exportFooter(DBRProgressMonitor monitor) throws DBException, IOException {
        if (writer != null) {
            try {
                writer.close();
            } catch (Exception e) {
                throw new DBException("Error closing Parquet writer", e);
            }
            writer = null;
        }
    }

    @Override
    public void dispose() {
        if (writer != null) {
            try {
                writer.close();
            } catch (Exception e) {
                log.error("Error disposing Parquet writer", e);
            }
            writer = null;
        }
        super.dispose();
    }

    @Nullable
    private Object convertValue(DBCSession session, @Nullable Object value) throws DBException {
        if (DBUtils.isNullValue(value)) {
            return null;
        }
        if (value instanceof DBDContent content) {
            try {
                DBDContentStorage cs = content.getContents(session.getProgressMonitor());
                if (cs == null) {
                    return null;
                }
                if (ContentUtils.isTextContent(content)) {
                    try (InputStream stream = cs.getContentStream()) {
                        return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
                    }
                } else {
                    try (InputStream stream = cs.getContentStream()) {
                        return stream.readAllBytes();
                    }
                }
            } finally {
                content.release();
            }
        }
        if (value instanceof Timestamp ts) {
            return ts.getTime() * 1000; // microseconds
        }
        if (value instanceof Date d) {
            return d.getTime();
        }
        if (value instanceof java.util.Date d) {
            return d.getTime();
        }
        return value;
    }

    @NotNull
    private MessageType buildSchema() {
        List<org.apache.parquet.schema.Type> fields = new ArrayList<>();
        for (DBDAttributeBinding col : columns) {
            String name = col.getName();
            if (CommonUtils.isEmpty(name)) {
                name = "column_" + fields.size();
            }
            fields.add(mapToParquetType(name, col));
        }
        return new MessageType("dbeaver_export", fields);
    }

    @NotNull
    private org.apache.parquet.schema.Type mapToParquetType(@NotNull String name, @NotNull DBDAttributeBinding col) {
        switch (col.getDataKind()) {
            case NUMERIC:
                return Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);
            case BOOLEAN:
                return Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name);
            case STRING:
                return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(org.apache.parquet.schema.OriginalType.UTF8)
                    .named(name);
            case DATETIME:
                return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                    .as(org.apache.parquet.schema.OriginalType.TIMESTAMP_MICROS)
                    .named(name);
            case BINARY:
                return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
            case CONTENT:
                return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(org.apache.parquet.schema.OriginalType.UTF8)
                    .named(name);
            default:
                return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(org.apache.parquet.schema.OriginalType.UTF8)
                    .named(name);
        }
    }

    private static class ObjectArrayWriteSupport extends WriteSupport<Object[]> {
        private final MessageType schema;
        private final DBDAttributeBinding[] columns;
        private final String nullString;
        private org.apache.parquet.io.api.RecordConsumer recordConsumer;

        ObjectArrayWriteSupport(MessageType schema, DBDAttributeBinding[] columns, String nullString) {
            this.schema = schema;
            this.columns = columns;
            this.nullString = nullString;
        }

        @Override
        public WriteContext init(Configuration configuration) {
            return new WriteContext(schema);
        }

        @Override
        public void prepareForWrite(org.apache.parquet.io.api.RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(Object[] row) {
            recordConsumer.startMessage();
            for (int i = 0; i < row.length; i++) {
                Object value = row[i];
                if (value == null) {
                    continue;
                }
                String fieldName = schema.getFieldName(i);
                recordConsumer.startField(fieldName, i);
                writeValue(value, columns[i]);
                recordConsumer.endField();
            }
            recordConsumer.endMessage();
        }

        private void writeValue(@NotNull Object value, @NotNull DBDAttributeBinding col) {
            switch (col.getDataKind()) {
                case NUMERIC:
                    recordConsumer.addDouble(((Number) value).doubleValue());
                    break;
                case BOOLEAN:
                    recordConsumer.addBoolean((Boolean) value);
                    break;
                case STRING:
                    recordConsumer.addBinary(
                        org.apache.parquet.io.api.Binary.fromString(value.toString())
                    );
                    break;
                case DATETIME:
                    if (value instanceof Long l) {
                        recordConsumer.addLong(l);
                    } else {
                        recordConsumer.addLong(((java.util.Date) value).getTime() * 1000);
                    }
                    break;
                case BINARY:
                    if (value instanceof byte[]) {
                        recordConsumer.addBinary(org.apache.parquet.io.api.Binary.fromConstantByteArray((byte[]) value));
                    } else {
                        recordConsumer.addBinary(
                            org.apache.parquet.io.api.Binary.fromString(value.toString())
                        );
                    }
                    break;
                default:
                    recordConsumer.addBinary(
                        org.apache.parquet.io.api.Binary.fromString(value.toString())
                    );
                    break;
            }
        }
    }

    private static class OutputStreamOutputFile implements OutputFile {
        private final OutputStream outputStream;

        OutputStreamOutputFile(OutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new PositionOutputStreamAdapter(outputStream);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return new PositionOutputStreamAdapter(outputStream);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

    private static class PositionOutputStreamAdapter extends PositionOutputStream {
        private final OutputStream out;
        private long pos = 0;

        PositionOutputStreamAdapter(OutputStream out) {
            this.out = out;
        }

        @Override
        public long getPos() throws IOException {
            return pos;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            pos++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            pos += len;
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }
}
