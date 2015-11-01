package com.jmlucjav.esURP;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory {
    private String esCluster;
    private String esIndex;
    private String esType;
    private String idField;
    private Set<String> ignoreFields;
    private boolean useTransportClient = true;
    private ESProcessor esProcessor;

    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
        if (idField == null) {
            idField = req.getSchema().getUniqueKeyField().toString();
            esProcessor.setIdField(idField);
        }
        return new EsUpdateRequestProcessor(next, esProcessor);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(NamedList args) {
        esCluster = getValue(args, "esCluster");
        esIndex = getValue(args, "esIndex");
        esType = getValue(args, "esType");
        useTransportClient = args.getBooleanArg("useTransportClient");
        String igns = (String) args.get("ignoreFields");
        ignoreFields = new HashSet<String>(Arrays.asList(igns.split(",")));
//      esProcessor = new ESProcessor(esCluster, esIndex, esType, ignoreFields, useTransportClient);
        esProcessor = new ESProcessorBulk(esCluster, esIndex, esType, ignoreFields, useTransportClient);
        super.init(args);
    }

    private String getValue(NamedList args, String arg) {
        Object d = args.remove(arg);
        if (null == d) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Init param '" + arg + "' must be specified.");
        } else if (!(d instanceof CharSequence)) {
            throw new SolrException
                    (SolrException.ErrorCode.SERVER_ERROR, "Init param '" + arg + "' must be a string ('str')");
        }
        return d.toString();
    }
}

class EsUpdateRequestProcessor extends UpdateRequestProcessor {
    private final static Logger log = LoggerFactory.getLogger(EsUpdateRequestProcessor.class);
    private ESProcessor esProcessor;

    public EsUpdateRequestProcessor(UpdateRequestProcessor next, ESProcessor esProcessor) {
        super(next);
        this.esProcessor = esProcessor;
        log.info(this.getClass().getSimpleName() + " will index to: " + esProcessor.toString());
    }


    //the only methods we need to override are Add/Delete/Commit
    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
        SolrInputDocument doc = cmd.getSolrInputDocument();
        synchronized (esProcessor) {
            esProcessor.add(doc);
        }
        // pass it up the chain, if we want to index just in ES, it is enough to remove the RunUpdateProcessorFactory in Solr config
        super.processAdd(cmd);
    }

    @Override
    public void processDelete(DeleteUpdateCommand cmd) throws IOException {
        String id = cmd.getId();
        if (id == null) {
            //only process *:* for now
            if ("*:*".equals(cmd.getQuery())) {
                synchronized (esProcessor) {
                    esProcessor.deleteAll();
                }
            } else {
                log.warn("Ignoring Delete by query: " + cmd.getQuery());
            }
        } else {
            synchronized (esProcessor) {
                esProcessor.delete(id);
            }
        }
        super.processDelete(cmd);
    }

    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
        synchronized (esProcessor) {
            esProcessor.commit();
        }
        super.processCommit(cmd);
    }


    @Override
    public void finish() throws IOException {
        synchronized (esProcessor) {
            esProcessor.close();
        }
    }
}

