/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.history;

import org.apache.http.HttpStatus;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapperResult;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionResult;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.history.WatchRecord;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.actions.jira.JiraAction;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.notification.jira.JiraAccount;
import org.elasticsearch.xpack.watcher.notification.jira.JiraIssue;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HistoryStoreTests extends ESTestCase {

    private HistoryStore historyStore;
    private Client client;

    @Before
    public void init() {
        Settings settings = Settings.builder().put("node.name", randomAlphaOfLength(10)).build();
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(settings));
        historyStore = createHistoryStore(null);
    }

    private HistoryStore createHistoryStore(ByteSizeValue maxHistoryRecordSize) {
        BulkProcessor2.Listener listener = mock(BulkProcessor2.Listener.class);
        BulkProcessor2 bulkProcessor = BulkProcessor2.builder(client::bulk, listener, client.threadPool()).setBulkActions(1).build();
        Settings.Builder settingsBuilder = Settings.builder();
        if (maxHistoryRecordSize != null) {
            settingsBuilder.put(HistoryStore.MAX_HISTORY_SIZE_SETTING.getKey(), maxHistoryRecordSize);
        }
        Settings settings = settingsBuilder.build();
        return new HistoryStore(bulkProcessor, settings);
    }

    public void testPut() throws Exception {
        ZonedDateTime now = Instant.ofEpochMilli(0).atZone(ZoneOffset.UTC);
        Wid wid = new Wid("_name", now);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(wid.watchId(), now, now);
        WatchRecord watchRecord = new WatchRecord.MessageWatchRecord(wid, event, ExecutionState.EXECUTED, null, randomAlphaOfLength(10));

        IndexResponse indexResponse = mock(IndexResponse.class);
        AtomicBoolean historyItemIndexed = new AtomicBoolean(false);
        doAnswer(invocation -> {
            BulkRequest request = (BulkRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) invocation.getArguments()[1];

            IndexRequest indexRequest = (IndexRequest) request.requests().get(0);
            if (indexRequest.id().equals(wid.value())
                && indexRequest.opType() == OpType.CREATE
                && indexRequest.index().equals(HistoryStoreField.DATA_STREAM)) {
                listener.onResponse(
                    new BulkResponse(new BulkItemResponse[] { BulkItemResponse.success(1, OpType.CREATE, indexResponse) }, 1)
                );
                historyItemIndexed.set(true);
            } else {
                listener.onFailure(new ElasticsearchException("test issue"));
                fail("Unexpected indexRequest");
            }
            return null;
        }).when(client).bulk(any(), any());

        historyStore.put(watchRecord);
        verify(client).bulk(any(), any());
        assertThat(historyItemIndexed.get(), equalTo(true));
    }

    @SuppressWarnings("unchecked")
    public void testPutLargeHistory() throws Exception {
        IndexResponse indexResponse = mock(IndexResponse.class);
        AtomicBoolean historyRedacted = new AtomicBoolean(false);
        doAnswer(invocation -> {
            BulkRequest request = (BulkRequest) invocation.getArguments()[0];
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) invocation.getArguments()[1];
            IndexRequest indexRequest = (IndexRequest) request.requests().get(0);
            Map<String, Object> sourceMap = indexRequest.sourceAsMap();
            if (indexRequest.opType() == OpType.CREATE && indexRequest.index().equals(HistoryStoreField.DATA_STREAM)) {
                if (sourceMap.containsKey("input")
                    && ((Map<String, Object>) sourceMap.get("input")).containsKey(WatchRecord.TRUNCATED_RECORD_KEY)
                    && sourceMap.containsKey("result")
                    && ((Map<String, Object>) sourceMap.get("result")).containsKey(WatchRecord.TRUNCATED_RECORD_KEY)) {
                    assertThat(
                        ((Map<String, Object>) sourceMap.get("input")).get(WatchRecord.TRUNCATED_RECORD_KEY),
                        equalTo(WatchRecord.TRUNCATED_RECORD_VALUE)
                    );
                    assertThat(
                        ((Map<String, Object>) sourceMap.get("result")).get(WatchRecord.TRUNCATED_RECORD_KEY),
                        equalTo(WatchRecord.TRUNCATED_RECORD_VALUE)
                    );
                    historyRedacted.set(true);
                }
                listener.onResponse(
                    new BulkResponse(new BulkItemResponse[] { BulkItemResponse.success(1, OpType.CREATE, indexResponse) }, 1)
                );
            } else {
                listener.onFailure(new ElasticsearchException("test issue"));
                fail("Should never get here");
            }
            return null;
        }).when(client).bulk(any(), any());
        HistoryStore historyStoreSmallLimit = createHistoryStore(ByteSizeValue.ofBytes(10));
        HistoryStore historyStoreLargeLimit = createHistoryStore(ByteSizeValue.ofBytes(10_000_000));
        {
            /*
             * First, create a history record with input and results. We expect this to not be truncated when the store has a high limit,
             * and we expect it to be truncated when we have the artificially low limit.
             */
            WatchExecutionContext context = WatcherTestUtils.createWatchExecutionContext();
            WatchExecutionResult result = new WatchExecutionResult(context, randomNonNegativeLong());
            String message = randomAlphaOfLength(100);
            WatchRecord watchRecord = new WatchRecord.MessageWatchRecord(context, result, message);
            historyStoreLargeLimit.put(watchRecord);
            verify(client, atLeastOnce()).bulk(any(), any());
            assertThat(historyRedacted.get(), equalTo(false));
            historyStoreSmallLimit.put(watchRecord);
            verify(client, atLeastOnce()).bulk(any(), any());
            assertThat(historyRedacted.get(), equalTo(true));
        }
        {
            /*
             * Now make sure that we don't blow up when the input and result are null
             */
            historyRedacted.set(false);
            ZonedDateTime now = Instant.ofEpochMilli(0).atZone(ZoneOffset.UTC);
            Wid wid = new Wid("_name", now);
            ScheduleTriggerEvent event = new ScheduleTriggerEvent(wid.watchId(), now, now);
            WatchRecord watchRecord = new WatchRecord.MessageWatchRecord(
                wid,
                event,
                ExecutionState.EXECUTED,
                null,
                randomAlphaOfLength(10)
            );
            historyStoreLargeLimit.put(watchRecord);
            verify(client, atLeastOnce()).bulk(any(), any());
            assertThat(historyRedacted.get(), equalTo(false));
            historyStoreSmallLimit.put(watchRecord);
            verify(client, atLeastOnce()).bulk(any(), any());
            assertThat(historyRedacted.get(), equalTo(false));
        }
        {
            /*
             * Now make sure that we don't blow up when the input and result are null
             */
            historyRedacted.set(false);
            WatchExecutionContext context = WatcherTestUtils.createWatchExecutionContext();
            WatchExecutionResult result = new WatchExecutionResult(context, randomNonNegativeLong());
            Exception exception = new RuntimeException(randomAlphaOfLength(100));
            WatchRecord watchRecord = new WatchRecord.ExceptionWatchRecord(context, result, exception);
            historyStoreLargeLimit.put(watchRecord);
            verify(client, atLeastOnce()).bulk(any(), any());
            assertThat(historyRedacted.get(), equalTo(false));
            historyStoreSmallLimit.put(watchRecord);
            verify(client, atLeastOnce()).bulk(any(), any());
            assertThat(historyRedacted.get(), equalTo(true));
        }
    }

    public void testStoreWithHideSecrets() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(new HttpResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR));

        final String username = randomFrom("admin", "elastic", "test");
        final String password = randomFrom("secret", "supersecret", "123456");
        final String url = "https://" + randomFrom("localhost", "internal-jira.elastic.co") + ":" + randomFrom(80, 8080, 449, 9443);

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("secure_url", url);
        secureSettings.setString("secure_user", username);
        secureSettings.setString("secure_password", password);
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        JiraAccount account = new JiraAccount("_account", settings, httpClient);

        JiraIssue jiraIssue = account.createIssue(singletonMap("foo", "bar"), null);
        ActionWrapperResult result = new ActionWrapperResult(JiraAction.TYPE, new JiraAction.Executed(jiraIssue));

        ZonedDateTime now = Instant.ofEpochMilli((long) 0).atZone(ZoneOffset.UTC);
        Wid wid = new Wid("_name", now);

        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        when(watch.status()).thenReturn(new WatchStatus(now, singletonMap("_action", new ActionStatus(now))));

        WatchExecutionContext context = mock(WatchExecutionContext.class);
        when(context.id()).thenReturn(wid);
        when(context.triggerEvent()).thenReturn(new ScheduleTriggerEvent(wid.watchId(), now, now));
        when(context.vars()).thenReturn(emptyMap());
        when(context.watch()).thenReturn(watch);

        WatchExecutionResult watchExecutionResult = new WatchExecutionResult(context, 0);

        WatchRecord watchRecord;
        if (randomBoolean()) {
            watchRecord = new WatchRecord.MessageWatchRecord(context, watchExecutionResult);
        } else {
            watchRecord = new WatchRecord.ExceptionWatchRecord(context, watchExecutionResult, new IllegalStateException());
        }
        watchRecord.result().actionsResults().put(JiraAction.TYPE, result);

        ArgumentCaptor<BulkRequest> requestCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        AtomicBoolean historyItemIndexed = new AtomicBoolean(false);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) invocation.getArguments()[1];

            IndexResponse indexResponse = mock(IndexResponse.class);
            listener.onResponse(new BulkResponse(new BulkItemResponse[] { BulkItemResponse.success(1, OpType.CREATE, indexResponse) }, 1));
            historyItemIndexed.set(true);
            return null;
        }).when(client).bulk(requestCaptor.capture(), any());

        if (randomBoolean()) {
            historyStore.put(watchRecord);
        } else {
            historyStore.forcePut(watchRecord);
        }
        assertThat(historyItemIndexed.get(), equalTo(true));
        assertThat(requestCaptor.getAllValues(), hasSize(1));
        assertThat(requestCaptor.getValue().requests().get(0), instanceOf(IndexRequest.class));
        IndexRequest capturedIndexRequest = (IndexRequest) requestCaptor.getValue().requests().get(0);
        String indexedJson = capturedIndexRequest.source().utf8ToString();
        assertThat(indexedJson, containsString(username));
        assertThat(indexedJson, not(containsString(password)));
    }
}
