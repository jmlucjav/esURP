package com.jmlucjav.esURP;

import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ESProcessorBulk extends ESProcessor {
    private BulkProcessor bulkProcessor;
    private final static Logger log = LoggerFactory.getLogger(ESProcessorBulk.class);


    public ESProcessorBulk(String esCluster, String esIndex, String esType, Set<String> ignoreFields, boolean useTransportClient) {
        super(esCluster, esIndex, esType, ignoreFields, useTransportClient);
        initBulk();
    }

    protected void initBulk() {
        //use a BulkProcessor to batch docs
        bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        log.debug(this.getClass().getSimpleName() + " Bulk " + request.toString() + " " + response.toString());
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        log.error(this.getClass().getSimpleName() + " error in bulk operation " + request.toString() + " " + failure.getCause() == null ? failure.getMessage() : failure.getCause().toString());
                    }
                })
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(60))
                .setConcurrentRequests(1)
                .build();
    }


    public void add(SolrInputDocument doc) {
        String id = getStringValue(doc, "id");
        XContentBuilder esdoc = createEsDoc(doc, null);
        bulkProcessor.add(new IndexRequest(esIndex, esType, id).source(esdoc));
        log.debug("doc add: " + id + ")");
    }

    public void delete(String id) {
        bulkProcessor.add(new DeleteRequest(esIndex, esType, id));
        log.debug("doc del: " + id);
    }

    public void commit() {
        bulkProcessor.flush();
        log.debug("flushed Bulk");
    }

    public void close() {
        bulkProcessor.flush();
        super.close();
    }
}

