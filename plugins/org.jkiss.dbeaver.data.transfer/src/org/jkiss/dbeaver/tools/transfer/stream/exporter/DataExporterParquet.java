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

import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataExporterParquet extends StreamExporterAbstract {

    private static final Log log = Log.getLog(DataExporterParquet.class);

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

    private static final Type mapType(org.jkiss.dbeaver.model.data.DBDValueKind kind) {
        return switch (kind) {
            case BOOLEAN -> Type.BOOLEAN;
            case NUMERIC -> Type.DOUBLE;
            case DATETIME -> Type.INT64;
            default -> Type.BYTE_ARRAY;
        };
    }

    private static final ConvertedType mapConvertedType(org.jkiss.dbeaver.model.data.DBDValueKind kind) {
        return switch (kind) {
            case STRING, CONTENT -> ConvertedType.UTF8;
            case DATETIME -> ConvertedType.TIMESTAMP_MICROS;
            default -> null;
        };
    }

    private DBDAttributeBinding[] columns;
    private List<Object[]> rows;
    private TSerializer serializer;

    @Override
    public void init(IStreamDataExporterSite site) throws DBException {
        super.init(site);
        rows = new ArrayList<>();
        serializer = new TSerializer(new TCompactProtocol.Factory());
    }

    @Override
    public void exportHeader(DBCSession session) throws DBException {
        columns = getSite().getAttributes();
        if (columns == null || columns.length == 0) {
            throw new DBException("No columns to export");
        }
    }

    @Override
    public void exportRow(DBCSession session, DBCResultSet resultSet, Object[] row) throws DBException {
        Object[] converted = new Object[row.length];
        for (int i = 0; i < row.length; i++) {
            converted[i] = convertValue(session, row[i]);
        }
        rows.add(converted);
    }

    @Override
    public void exportFooter(DBRProgressMonitor monitor) throws DBException, IOException {
        if (rows.isEmpty()) {
            return;
        }

        int numRows = rows.size();
        List<SchemaElement> schema = buildSchema();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        baos.write(MAGIC);

        List<ColumnChunk> columnChunks = new ArrayList<>();
        long fileOffset = 4;

        for (int ci = 0; ci < columns.length; ci++) {
            ByteArrayOutputStream pageData = new ByteArrayOutputStream();
            DataOutputStream pd = new DataOutputStream(pageData);

            int nonNullCount = 0;
            byte[] defLevels = new byte[(numRows + 7) / 8];
            int bitIndex = 0;

            for (Object[] row : rows) {
                Object val = row[ci];
                boolean isNull = val == null;
                if (!isNull) {
                    encodePlainValue(pd, columns[ci], val);
                    nonNullCount++;
                }
                if (isNull) {
                    defLevels[bitIndex / 8] &= ~(1 << (7 - (bitIndex % 8)));
                } else {
                    defLevels[bitIndex / 8] |= (1 << (7 - (bitIndex % 8)));
                }
                bitIndex++;
            }

            byte[] rawData = pageData.toByteArray();

            ByteArrayOutputStream pageBytes = new ByteArrayOutputStream();
            DataOutputStream po = new DataOutputStream(pageBytes);

            po.write(1);
            int numGroups = (numRows + 7) / 8;
            writeVarint(po, (numGroups << 1) | 1);
            po.write(defLevels, 0, numGroups);

            po.write(rawData);

            byte[] pageContent = pageBytes.toByteArray();

            DataPageHeader dpHeader = new DataPageHeader(
                nonNullCount,
                Encoding.PLAIN,
                Encoding.RLE,
                Encoding.RLE
            );

            PageHeader pageHeader = new PageHeader(
                PageType.DATA_PAGE,
                pageContent.length,
                pageContent.length
            );
            pageHeader.setData_page_header(dpHeader);

            byte[] headerBytes = serializer.serialize(pageHeader);

            baos.write(headerBytes);
            baos.write(pageContent);

            ColumnMetaData meta = new ColumnMetaData(
                mapType(columns[ci].getDataKind()),
                List.of(Encoding.PLAIN, Encoding.RLE),
                CompressionCodec.UNCOMPRESSED,
                nonNullCount,
                headerBytes.length + pageContent.length,
                headerBytes.length + pageContent.length,
                fileOffset
            );
            meta.setNull_count((long) (numRows - nonNullCount));

            ColumnChunk chunk = new ColumnChunk(fileOffset);
            chunk.setMeta_data(meta);
            columnChunks.add(chunk);

            fileOffset += headerBytes.length + pageContent.length;
        }

        RowGroup rowGroup = new RowGroup(columnChunks, numRows);
        rowGroup.setTotal_byte_size(fileOffset - 4);

        FileMetaData metadata = new FileMetaData(schema, numRows, List.of(rowGroup));
        metadata.setCreated_by("DBeaver CE");
        metadata.setVersion(2);

        byte[] footerBytes = serializer.serialize(metadata);

        baos.write(footerBytes);

        ByteBuffer lenBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        lenBuf.putInt(footerBytes.length);
        baos.write(lenBuf.array());
        baos.write(MAGIC);

        baos.writeTo(getOutputStream());
        getOutputStream().flush();
        rows.clear();
    }

    @Override
    public void dispose() {
        if (rows != null) {
            rows.clear();
            rows = null;
        }
        serializer = null;
        super.dispose();
    }

    @NotNull
    private List<SchemaElement> buildSchema() {
        List<SchemaElement> schema = new ArrayList<>();
        SchemaElement root = new SchemaElement("dbeaver_export");
        root.setRepetition_type(FieldRepetitionType.REQUIRED);
        schema.add(root);

        for (DBDAttributeBinding col : columns) {
            SchemaElement se = new SchemaElement(col.getName());
            se.setRepetition_type(FieldRepetitionType.OPTIONAL);
            se.setNum_children(0);
            se.setType(mapType(col.getDataKind()));
            ConvertedType ct = mapConvertedType(col.getDataKind());
            if (ct != null) {
                se.setConverted_type(ct);
            }
            schema.add(se);
        }
        return schema;
    }

    @Nullable
    private Object convertValue(DBCSession session, @Nullable Object value) throws DBException {
        if (DBUtils.isNullValue(value)) return null;
        if (value instanceof DBDContent content) {
            try {
                DBDContentStorage cs = content.getContents(session.getProgressMonitor());
                if (cs == null) return null;
                if (ContentUtils.isTextContent(content)) {
                    try (InputStream is = cs.getContentStream()) {
                        return new String(is.readAllBytes(), StandardCharsets.UTF_8);
                    }
                } else {
                    try (InputStream is = cs.getContentStream()) {
                        return is.readAllBytes();
                    }
                }
            } finally {
                content.release();
            }
        }
        if (value instanceof Timestamp ts) return ts.getTime() * 1000;
        if (value instanceof Date d) return d.getTime();
        if (value instanceof java.util.Date d) return d.getTime();
        return value;
    }

    private void encodePlainValue(DataOutputStream out, DBDAttributeBinding col, Object value) throws IOException {
        switch (col.getDataKind()) {
            case NUMERIC -> out.writeDouble(((Number) value).doubleValue());
            case BOOLEAN -> out.writeBoolean((Boolean) value);
            case DATETIME -> {
                long micros = value instanceof Long lv ? lv : ((java.util.Date) value).getTime() * 1000;
                out.writeLong(micros);
            }
            case BINARY -> {
                byte[] bytes = value instanceof byte[] ba ? ba : value.toString().getBytes(StandardCharsets.UTF_8);
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            default -> {
                byte[] strBytes = value.toString().getBytes(StandardCharsets.UTF_8);
                out.writeInt(strBytes.length);
                out.write(strBytes);
            }
        }
    }

    private void writeVarint(DataOutputStream out, int value) throws IOException {
        while ((value & ~0x7F) != 0) {
            out.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.writeByte((byte) value);
    }
}
