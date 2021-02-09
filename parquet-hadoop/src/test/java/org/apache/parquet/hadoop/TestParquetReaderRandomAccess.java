/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.junit.Assert.*;

public class TestParquetReaderRandomAccess {
  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType("" +
    "message m {" +
    "  required group a {" +
    "    required binary b;" +
    "  }" +
    "  required group c {" +
    "    required int64 d;" +
    "  }" +
    "}");
  private static final String[] PATH1 = {"a", "b"};
  private static final ColumnDescriptor C1 = SCHEMA.getColumnDescription(PATH1);
  private static final String[] PATH2 = {"c", "d"};
  private static final ColumnDescriptor C2 = SCHEMA.getColumnDescription(PATH2);

  private static final byte[] BYTES1 = { 0, 1, 2, 3 };
  private static final byte[] BYTES2 = { 1, 2, 3, 4 };
  private static final byte[] BYTES3 = { 2, 3, 4, 5 };
  private static final byte[] BYTES4 = { 3, 4, 5, 6 };
  private static final CompressionCodecName CODEC = CompressionCodecName.UNCOMPRESSED;

  private static final org.apache.parquet.column.statistics.Statistics<?> EMPTY_STATS = org.apache.parquet.column.statistics.Statistics
    .getBuilderForReading(Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named("test_binary")).build();

  private void writeDataToFile(Path path) throws IOException {
    Configuration configuration = new Configuration();

    ParquetFileWriter w = new ParquetFileWriter(configuration, SCHEMA, path);
    w.start();
    w.startBlock(3);
    w.startColumn(C1, 5, CODEC);
    w.writeDataPage(2, 4, BytesInput.from(BYTES1), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(C2, 6, CODEC);
    w.writeDataPage(2, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(3, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.writeDataPage(1, 4, BytesInput.from(BYTES2), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();
    w.startBlock(4);
    w.startColumn(C1, 7, CODEC);
    w.writeDataPage(7, 4, BytesInput.from(BYTES3), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.startColumn(C2, 8, CODEC);
    w.writeDataPage(8, 4, BytesInput.from(BYTES4), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();
    w.end(new HashMap<String, String>());
  }

  @Test
  public void testReadRandom() throws IOException {
    File testFile = temp.newFile();
    testFile.delete();
    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();
    ParquetReadOptions options = ParquetReadOptions.builder().build();

    writeDataToFile(path);

    ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(path, configuration), options);
    List<BlockMetaData> blocks = reader.getRowGroups();

    // Randomize indexes
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < blocks.size(); i++) {
      for (int j = 0; j < 4; j++) {
        indexes.add(i);
      }
    }

    Collections.shuffle(indexes);

    for (int index : indexes) {
      PageReadStore pages = reader.readRowGroup(blocks.get(index));
      validatePages(pages, index);
    }
  }

  @Test
  public void testReadRandomAndSequential() throws IOException {
    File testFile = temp.newFile();
    testFile.delete();
    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();
    ParquetReadOptions options = ParquetReadOptions.builder().build();

    writeDataToFile(path);

    ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(path, configuration), options);
    List<BlockMetaData> blocks = reader.getRowGroups();

    // Randomize indexes
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < blocks.size(); i++) {
      for (int j = 0; j < 4; j++) {
        indexes.add(i);
      }
    }

    Collections.shuffle(indexes);

    int splitPoint = indexes.size()/2;

    {
      PageReadStore pages = reader.readNextRowGroup();
      validatePages(pages, 0);
    }
    for (int i = 0; i < splitPoint; i++) {
      int index = indexes.get(i);
      PageReadStore pages = reader.readRowGroup(blocks.get(index));
      validatePages(pages, index);
    }
    {
      PageReadStore pages = reader.readNextRowGroup();
      validatePages(pages, 1);
    }
    for (int i = splitPoint; i < indexes.size(); i++) {
      int index = indexes.get(i);
      PageReadStore pages = reader.readRowGroup(blocks.get(index));
      validatePages(pages, index);
    }
    {
      PageReadStore pages = reader.readNextRowGroup();
      validatePages(pages, 2);
    }
  }

  private void validatePages(PageReadStore pages, int index) throws IOException {
    if (index == 0) {
      assertEquals(3, pages.getRowCount());
      validateContains(pages, PATH1, 2, BytesInput.from(BYTES1));
      validateContains(pages, PATH1, 3, BytesInput.from(BYTES1));
      validateContains(pages, PATH2, 2, BytesInput.from(BYTES2));
      validateContains(pages, PATH2, 3, BytesInput.from(BYTES2));
      validateContains(pages, PATH2, 1, BytesInput.from(BYTES2));
    } else if (index == 1) {
      assertEquals(4, pages.getRowCount());
      validateContains(pages, PATH1, 7, BytesInput.from(BYTES3));
      validateContains(pages, PATH2, 8, BytesInput.from(BYTES4));
    } else {
      assertNull(pages);
    }
  }

  private void validateContains(PageReadStore pages, String[] path, int values, BytesInput bytes) throws IOException {
    PageReader pageReader = pages.getPageReader(SCHEMA.getColumnDescription(path));
    DataPage page = pageReader.readPage();
    assertEquals(values, page.getValueCount());
    assertArrayEquals(bytes.toByteArray(), ((DataPageV1)page).getBytes().toByteArray());
  }

}
