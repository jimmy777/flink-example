package com.yzy.flink.file;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;

/**
 * @Author Y.Z.Y
 * @Date 2020/12/25 10:30
 * @Description
 */
public class ParquetAvroWritersWithCompress {

    public static <T extends SpecificRecordBase> ParquetWriterFactory<T> forSpecificRecord(Class<T> type, CompressionCodecName com) {
        String schemaString = SpecificData.get().getSchema(type).toString();
        ParquetBuilder<T> builder = (out) -> {
            return createAvroParquetWriter(schemaString, SpecificData.get(), out, com);
        };
        return new ParquetWriterFactory(builder);
    }

    public static ParquetWriterFactory<GenericRecord> forGenericRecord(Schema schema, CompressionCodecName com) {
        String schemaString = schema.toString();
        ParquetBuilder<GenericRecord> builder = (out) -> {
            return createAvroParquetWriter(schemaString, GenericData.get(), out, com);
        };
        return new ParquetWriterFactory(builder);
    }

    public static <T> ParquetWriterFactory<T> forReflectRecord(Class<T> type, CompressionCodecName com) {
        String schemaString = ReflectData.get().getSchema(type).toString();
        ParquetBuilder<T> builder = (out) -> {
            return createAvroParquetWriter(schemaString, ReflectData.get(), out, com);
        };
        return new ParquetWriterFactory(builder);
    }

    private static <T> ParquetWriter<T> createAvroParquetWriter(String schemaString, GenericData dataModel, OutputFile out, CompressionCodecName com) throws IOException {
        Schema schema = (new Schema.Parser()).parse(schemaString);
        return AvroParquetWriter.<T>builder(out).withSchema(schema).withDataModel(dataModel).withCompressionCodec(com).build();
    }

    public ParquetAvroWritersWithCompress() {
    }
}
