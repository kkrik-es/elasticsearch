/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ColumnarLogsdbIndexModeTests extends ESTestCase {

    public void testColumnarLogsdbFromString() {
        assertThat(IndexMode.fromString("columnar_logsdb"), equalTo(IndexMode.COLUMNAR_LOGSDB));
        assertThat(IndexMode.fromString("COLUMNAR_LOGSDB"), equalTo(IndexMode.COLUMNAR_LOGSDB));
    }

    public void testColumnarLogsdbSerialization() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            IndexMode.writeTo(IndexMode.COLUMNAR_LOGSDB, out);
            try (var in = out.bytes().streamInput()) {
                assertThat(IndexMode.readFrom(in), equalTo(IndexMode.COLUMNAR_LOGSDB));
            }
        }
    }

    public void testColumnarLogsdbIndexModeSetting() {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR_LOGSDB.getName()).build();
        assertThat(IndexSettings.MODE.get(settings), equalTo(IndexMode.COLUMNAR_LOGSDB));
    }

    public void testColumnarLogsdbDefaultSourceMode() {
        assertThat(IndexMode.COLUMNAR_LOGSDB.defaultSourceMode(), equalTo(SourceFieldMapper.Mode.SYNTHETIC));
    }

    public void testColumnarLogsdbGetName() {
        assertThat(IndexMode.COLUMNAR_LOGSDB.getName(), equalTo("columnar_logsdb"));
        assertThat(IndexMode.COLUMNAR_LOGSDB.toString(), equalTo("columnar_logsdb"));
    }

    public void testColumnarLogsdbShouldValidateTimestamp() {
        assertThat(IndexMode.COLUMNAR_LOGSDB.shouldValidateTimestamp(), equalTo(false));
    }

    public void testColumnarLogsdbTimestampBound() {
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta(
            "test",
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR_LOGSDB.getName()).build()
        );
        assertThat(IndexMode.COLUMNAR_LOGSDB.getTimestampBound(metadata), equalTo(null));
    }
}
