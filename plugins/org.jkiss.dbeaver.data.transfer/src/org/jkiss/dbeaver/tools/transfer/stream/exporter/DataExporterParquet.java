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
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.model.DBPDataKind;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.data.DBDAttributeBinding;
import org.jkiss.dbeaver.model.data.DBDContent;
import org.jkiss.dbeaver.model.data.DBDContentStorage;
import org.jkiss.dbeaver.model.exec.DBCResultSet;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.tools.transfer.stream.IStreamDataExporterSite;
import org.jkiss.dbeaver.utils.ContentUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class DataExporterParquet extends StreamExporterAbstract {

    private static final Log log = Log.getLog(DataExporterParquet.class);

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

    private static final Type mapType(DBPDataKind kind) {
        return switch (kind) {
            case BOOLEAN -> Type.BOOLEAN;
            case NUMERIC -> Type.DOUBLE;
            case DATETIME -> Type.INT64;
            default -> Type.BYTE_ARRAY;
        };
    }

    private static final ConvertedType mapConvertedType(DBPDataKind kind) {
        return switch (kind) {
            case STRING, CONTENT -> ConvertedType.UTF8;
            case DATETIME -> ConvertedType.TIMESTAMP_MICROS;
            default -> null;
        };
    }

    private DBDAttributeBinding[] columns;
    private List<Object[]> rows;
    private CompressionCodec compressionCodec;

    @Override
    public void init(IStreamDataExporterSite site) throws DBException {
        super.init(site);
        rows = new ArrayList<>();
        compressionCodec = parseCompressionCodec(site);
    }

    private static CompressionCodec parseCompressionCodec(IStreamDataExporterSite site) throws DBException {
        Object compProp = site.getProperties().get("compression");
        if (compProp == null) {
            return CompressionCodec.UNCOMPRESSED;
        }
        String compStr = compProp.toString().trim();
        if (compStr.isEmpty()) {
            return CompressionCodec.UNCOMPRESSED;
        }
        try {
            CompressionCodec codec = CompressionCodec.valueOf(compStr);
            switch (codec) {
                case UNCOMPRESSED:
                case GZIP:
                    return codec;
                default:
                    log.warn("Compression codec " + compStr + " is not supported, using UNCOMPRESSED");
                    return CompressionCodec.UNCOMPRESSED;
            }
        } catch (IllegalArgumentException e) {
            throw new DBException("Unknown compression codec: " + compStr);
        }
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
        if (rows.isEmpty() || columns == null) {
            // Emit a valid empty Parquet file: magic + footer(metadata with schema and 0 rows) + footer length + magic
            List<SchemaElement> schema;
            if (columns == null) {
                schema = new ArrayList<>();
                SchemaElement root = new SchemaElement("dbeaver_export");
                root.setRepetition_type(FieldRepetitionType.REQUIRED);
                root.setNum_children(0);
                schema.add(root);
            } else {
                schema = buildSchema();
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write(MAGIC);
            FileMetaData metadata = new FileMetaData(2, schema, 0, new ArrayList<>());
            metadata.setCreated_by("DBeaver CE");
            byte[] footerBytes;
            try {
                footerBytes = serializeThrift(metadata);
            } catch (TException e) {
                throw new IOException("Failed to serialize footer metadata", e);
            }
            baos.write(footerBytes);
            ByteBuffer lenBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            lenBuf.putInt(footerBytes.length);
            baos.write(lenBuf.array());
            baos.write(MAGIC);
            baos.writeTo(getOutputStream());
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

            byte[] defLevelBits = new byte[(numRows + 7) / 8];
            int valuesCount = 0;

            for (int ri = 0; ri < numRows; ri++) {
                Object val = rows.get(ri)[ci];
                boolean isNull = val == null;
                if (!isNull) {
                    encodePlainValue(pd, columns[ci], val);
                    defLevelBits[ri / 8] |= (1 << (ri % 8));
                    valuesCount++;
                }
            }

            byte[] rawData = pageData.toByteArray();

            ByteArrayOutputStream pageBytes = new ByteArrayOutputStream();
            DataOutputStream po = new DataOutputStream(pageBytes);

            // Definition levels: RLE/bit-pack hybrid <4-byte container length> <runs>
            // Per Parquet spec, the bit-width is NOT embedded in page data;
            // it is derived from max definition level (pass as parameter to Decode).
            // The container length allows the reader to skip to value data.
            ByteArrayOutputStream rleBuf = new ByteArrayOutputStream();
            DataOutputStream rleOut = new DataOutputStream(rleBuf);
            int numGroups = (numRows + 7) / 8;
            writeVarint(rleOut, (numGroups << 1) | 1);
            for (int i = 0; i < numGroups; i++) {
                rleOut.write(defLevelBits[i]);
            }
            byte[] rleBytes = rleBuf.toByteArray();
            po.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(rleBytes.length).array());
            po.write(rleBytes);

            po.write(rawData);

            byte[] pageContent = pageBytes.toByteArray();

            byte[] compressedContent;
            if (compressionCodec == CompressionCodec.GZIP) {
                ByteArrayOutputStream cbos = new ByteArrayOutputStream();
                try (GZIPOutputStream gzipOut = new GZIPOutputStream(cbos)) {
                    gzipOut.write(pageContent);
                }
                compressedContent = cbos.toByteArray();
            } else {
                compressedContent = pageContent;
            }

            DataPageHeader dpHeader = new DataPageHeader(
                numRows,
                Encoding.RLE,
                Encoding.RLE,
                Encoding.PLAIN
            );

            PageHeader pageHeader = new PageHeader(
                PageType.DATA_PAGE,
                pageContent.length,
                compressedContent.length
            );
            pageHeader.setData_page_header(dpHeader);

            byte[] headerBytes;
            try {
                headerBytes = serializeThrift(pageHeader);
            } catch (TException e) {
                throw new IOException("Failed to serialize page header", e);
            }

            baos.write(headerBytes);
            baos.write(compressedContent);

            ColumnMetaData meta = new ColumnMetaData(
                mapType(columns[ci].getDataKind()),
                List.of(Encoding.PLAIN, Encoding.RLE),
                List.of(columns[ci].getName()),
                compressionCodec,
                numRows,
                pageContent.length,
                compressedContent.length,
                fileOffset
            );

            ColumnChunk chunk = new ColumnChunk(fileOffset);
            chunk.setMeta_data(meta);
            columnChunks.add(chunk);

            fileOffset += headerBytes.length + compressedContent.length;
        }

        long totalByteSize = fileOffset - 4;
        RowGroup rowGroup = new RowGroup(columnChunks, totalByteSize, numRows);

        FileMetaData metadata = new FileMetaData(2, schema, numRows, List.of(rowGroup));
        metadata.setCreated_by("DBeaver CE");

        byte[] footerBytes;
        try {
            footerBytes = serializeThrift(metadata);
        } catch (TException e) {
            throw new IOException("Failed to serialize footer metadata", e);
        }

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
        super.dispose();
    }

    private byte[] serializeThrift(shaded.parquet.org.apache.thrift.TBase<?, ?> struct) throws TException, IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        TIOStreamTransport transport = new TIOStreamTransport(buf);
        TCompactProtocol protocol = new TCompactProtocol(transport);
        struct.write(protocol);
        return buf.toByteArray();
    }

    @NotNull
    private List<SchemaElement> buildSchema() {
        List<SchemaElement> schema = new ArrayList<>();
        SchemaElement root = new SchemaElement("dbeaver_export");
        root.setRepetition_type(FieldRepetitionType.REQUIRED);
        root.setNum_children(columns.length);
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
                try {
                    if (ContentUtils.isTextContent(content)) {
                        try (InputStream is = cs.getContentStream()) {
                            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
                        }
                    } else {
                        try (InputStream is = cs.getContentStream()) {
                            return is.readAllBytes();
                        }
                    }
                } catch (IOException e) {
                    throw new DBException("Error reading content value", e);
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
            case NUMERIC -> {
                double d;
                if (value instanceof Number n) {
                    d = n.doubleValue();
                } else {
                    try {
                        d = Double.parseDouble(value.toString());
                    } catch (NumberFormatException e) {
                        d = 0;
                    }
                }
                writeLongLE(out, Double.doubleToLongBits(d));
            }
            case BOOLEAN -> {
                out.write(value instanceof Boolean b && b || Boolean.parseBoolean(value.toString()) ? 1 : 0);
            }
            case DATETIME -> {
                long micros;
                if (value instanceof Long lv) {
                    micros = lv;
                } else if (value instanceof java.util.Date d) {
                    micros = d.getTime() * 1000;
                } else {
                    try {
                        micros = Long.parseLong(value.toString());
                    } catch (NumberFormatException e) {
                        micros = 0;
                    }
                }
                writeLongLE(out, micros);
            }
            case BINARY -> {
                byte[] bytes = value instanceof byte[] ba ? ba : value.toString().getBytes(StandardCharsets.UTF_8);
                writeIntLE(out, bytes.length);
                out.write(bytes);
            }
            default -> {
                byte[] strBytes = value.toString().getBytes(StandardCharsets.UTF_8);
                writeIntLE(out, strBytes.length);
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

    private void writeIntLE(DataOutputStream out, int v) throws IOException {
        out.write(v & 0xFF);
        out.write((v >> 8) & 0xFF);
        out.write((v >> 16) & 0xFF);
        out.write((v >> 24) & 0xFF);
    }

    private void writeLongLE(DataOutputStream out, long v) throws IOException {
        out.write((int) (v & 0xFF));
        out.write((int) ((v >> 8) & 0xFF));
        out.write((int) ((v >> 16) & 0xFF));
        out.write((int) ((v >> 24) & 0xFF));
        out.write((int) ((v >> 32) & 0xFF));
        out.write((int) ((v >> 40) & 0xFF));
        out.write((int) ((v >> 48) & 0xFF));
        out.write((int) ((v >> 56) & 0xFF));
    }
}
