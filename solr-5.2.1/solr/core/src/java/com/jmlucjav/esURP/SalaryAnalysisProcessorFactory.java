package com.jmlucjav.esURP;

import java.io.IOException;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SalaryAnalysisProcessorFactory extends UpdateRequestProcessorFactory {

    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
        return new SalaryAnalysisProcessor(next);
    }
}

class SalaryAnalysisProcessor extends UpdateRequestProcessor {
    private final static Logger log = LoggerFactory.getLogger(SalaryAnalysisProcessor.class);

    public SalaryAnalysisProcessor(UpdateRequestProcessor next) {
        super(next);
        log.info(this.getClass().getSimpleName() + " initialized");
    }


    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
        SolrInputDocument sd = cmd.getSolrInputDocument();
        //child docs
        List<SolrInputDocument> children = sd.getChildDocuments();
        if (children != null) {
            //they are ordered by from_date
            int increase = 0;
            float increasePerc = 0;
            SolrInputDocument prev = null;
            int salaryPrev = 0;
            for (SolrInputDocument d : children) {
                int salary = (int) d.getFieldValue("salary");
                if (prev != null) {
                    int tinc = salary - salaryPrev;
                    if (tinc > increase) {
                        increase = tinc;
                    }
                    float tperc = (float) tinc / (float) salaryPrev;
                    if (tperc > increasePerc) {
                        increasePerc = tperc;
                    }
                }
                prev = d;
                salaryPrev = salary;
            }
            sd.addField("increase", increase);
            sd.addField("increasePerc", increasePerc);
        }

        super.processAdd(cmd);
    }
}


