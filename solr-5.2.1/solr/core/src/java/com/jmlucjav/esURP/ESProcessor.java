package com.jmlucjav.esURP;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ESProcessor {
    protected String esCluster;
    protected String esIndex;
    protected String esType;
    protected String idField;
    protected Set<String> ignoreFields;
    protected Client client;
    private final static Logger log = LoggerFactory.getLogger(ESProcessor.class);

    public ESProcessor(String esCluster, String esIndex, String esType, Set<String> ignoreFields, boolean useTransportClient) {
        this.esCluster = esCluster;
        this.esIndex = esIndex;
        this.esType = esType;
        this.ignoreFields = ignoreFields;
        client = ESClientHelper.getESClient(esCluster, useTransportClient);
    }

    public void add(SolrInputDocument doc) {
        String id = getStringValue(doc, "id");
        XContentBuilder esdoc = createEsDoc(doc, null);
        IndexResponse response = client.prepareIndex(esIndex, esType, id)
                .setSource(esdoc)
                .execute()
                .actionGet();
        log.debug("doc add: " + id + " (" + esType + ") Resp:" + response.toString());
    }

    public void delete(String id) {
        DeleteResponse response = client.prepareDelete(esIndex, esType, id)
                .execute()
                .actionGet();
        log.debug("doc del: " + id + " Resp:" + response.toString());
    }

    public void deleteAll() {
        //there is no Java api to delete by query, so use admin
        try {
            DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest(esIndex)).actionGet();
            if (!delete.isAcknowledged()) {
                log.error("Index wasn't deleted");
            }
        } catch (IndexNotFoundException e) {
            //no problem, ignore it
        }
        //we use this operation to setup needed mappings for next adds too. This is somewhat customized for how DIH works
        //now we create the index again
        createIndex();
    }

    private void createIndex() {
        final XContentBuilder mappingBuilder;
        try {
            CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(esIndex);
            //tweak the mapping: use Nested objects
            mappingBuilder = jsonBuilder().startObject().startObject(esType)
                    //disable _all
                    .startObject("_all").field("enabled", "false").endObject()
                    .startObject("properties")
//                    .startObject("_childDocuments_").field("type", "nested").field("include_in_parent", "false").endObject()
                    .startObject("_childDocuments_").field("type", "nested").endObject()
                    .endObject()
                    .endObject().endObject();
            createIndexRequestBuilder.addMapping(esType, mappingBuilder);
            createIndexRequestBuilder.execute().actionGet();
        } catch (IOException e) {
            log.error("Creation of index failed: " + e.getLocalizedMessage());
        }
    }

    public void commit() {
        //nothing to do
        log.debug("commit");
    }

    protected XContentBuilder createEsDoc(SolrInputDocument sd, XContentBuilder xContent) {
        XContentBuilder lx = null;
        try {
            if (xContent == null) {
                lx = XContentFactory.jsonBuilder().startObject();
            } else {
                //child doc, in Solr this is member of _childDocuments_
                lx = xContent.startObject();
            }
            for (String fname : sd.getFieldNames()) {
                SolrInputField sf = sd.get(fname);
                if (!ignoreFields.contains(fname)) {
                    if (sf.getValueCount() == 1) {
                        if (idField.equals(fname)) {
                            //ES id is _id
                            lx.field("_id", sf.getValue());
                        } else {
                            lx.field(fname, sf.getValue());
                        }
                    } else {
                        //multivalue
                        Collection<Object> c = sf.getValues();
                        lx.field(fname, c);
                    }
                }
            }
            //child docs
            List<SolrInputDocument> children = sd.getChildDocuments();
            if (children != null) {
                lx.startArray("_childDocuments_");
                for (SolrInputDocument d : children) {
                    createEsDoc(d, lx);
                }
                lx.endArray();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (xContent == null) {
                if (lx != null) {
                    lx.close();
                }
            } else {
                //this is a child, dont close, just end
                try {
                    lx.endObject();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            //debug it, only call .string() when we are done with top level object, if done in a child object we mutate the state and next child will fail
            if (xContent == null) {
                String xstring = lx.string();
                log.debug("ES doc: " + xstring);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lx;
    }

//    protected XContentBuilder createEsDoc(SolrInputDocument sd, XContentBuilder xContent) {
//        try {
//            xContent = XContentFactory.jsonBuilder().startObject();
//            for (String fname : sd.getFieldNames()) {
//                SolrInputField sf = sd.get(fname);
//                if (sf.getValueCount() == 1) {
//                    xContent.field(fname, sf.getValue());
//                } else {
//                    //multivalue
//                    Collection<Object> c = sf.getValues();
//                    xContent.startArray("attributes");
//                    for (Object o : c) {
//                        xContent.startObject();
//                        xContent.field(fname, o);
//                        xContent.endObject();
//                    }
//                    xContent.endArray();
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (xContent != null)
//                xContent.close();
//        }
//        return xContent;
//    }
//

    protected String getStringValue(SolrInputDocument doc, String f) {
        String id = null;
        try {
            id = (String) doc.getFieldValue(f);
        } catch (Throwable e) {
            //any other type...
            Object o = doc.getFieldValue(f);
            if (o != null) {
                id = o.toString();
            }
        }
        return id;
    }

    public String toString() {
        return esCluster + " index:" + esIndex + " type:" + esType;
    }

    public void close() {
        ESClientHelper.closeClient(client);
    }


    public void setIdField(String idField) {
        this.idField = idField;
    }
}
