# esURP: use Solr's DataImportHandler and UpdateRequestProcessor in ElasticSearch

Description
---------------

This is implemented as a Solr UpdateRequestProcessor (URP) that redirects docs to ES. It is thoroughly explained [in this blog post](https://medium.com/@jmlucjav/using-solr-s-dataimporthandler-and-updaterequestprocessor-in-elasticsearch-2-0-596eb6e3a483).

Using this, you should be able to configure your current Solr instance so it points to an ES instance and:

- you can then index via DIH to ES
- you can index to ES after the docs have been processed by any URP you want
- both of the above
- docs can also be indexed on Solr (at the same time as in ES)
- it has been tested with Solr5.2.1 and ES2.0. But should work fine with newer versions too. Important: ES must be using the same Lucene version Solr is using.

Usage
-------------------
**On ElasticSearch side**: just start ES normally.

**On Solr side**:

- add the following (or corresponding, if you are using a diff version than ES2.0) jars from ES to solr\server\solr-webapp\webapp\WEB-INF\lib\:
    elasticsearch-2.0.0-beta1-SNAPSHOT.jar
    jackson-core-2.5.3.jar
    jackson-dataformat-yaml-2.5.3.jar
    jsr166e-1.1.0.jar
    guava-18.0.jar
    hppc-0.7.1.jar
    netty-3.10.3.Final.jar
    jna-4.1.0.jar
    compress-lzf-1.0.2.jar
- removed the original jars from Solr that are superseeded by those just copied, in my case:
    guava-14.0.1.jar
    hppc-0.5.2.jar
- also add EsUpdateRequestProcessorFactory classes to Solr. I run them from my IDE, but you can create a jar too and put it with the ones above
- configure solrconfig.xml so the chain that handled the docs you want to index in ES are processed by EsUpdateRequestProcessorFactory, for example with this configuration, we would be able to index into ES using DIH:

```

    <updateRequestProcessorChain name="mychain">
    <processor class="com.jmlucjav.esURP.EsUpdateRequestProcessorFactory">
        <str name="esCluster">elasticsearch</str>
        <str name="esIndex">employees</str>
        <str name="esType">employee</str>
        <str name="ignoreFields">parent</str>
        <bool name="useTransportClient">false</bool>
    </processor>
    <processor class="solr.RunUpdateProcessorFactory"/>
    </updateRequestProcessorChain>

    <!-- DIH -->
    <requestHandler name="/dataimport" class="solr.DataImportHandler">
        <lst name="defaults">
          <str name="config">db-data-config.xml</str>
          <str name="update.chain">mychain</str>
        </lst>
    </requestHandler>
```

  The parameters above are quite straighforward, the indicate what the ES cluster, index and type. And allow you to ignore certain document fields so they are not sent to ES.

- start Solr this way:

```
solr/bin/solr start -a "-Des.path.home=path-to-es\elasticsearch-1.7.1 -Des.security.manager.enabled=false"
```

Now just index docs in Solr, and they will show up in ES. 


Limitations
----------------

- the ES mappings needed (for Nested types etc) are configured when a full delete is done from Solr. This was handy cause DIH sends a full delete when reindexing. If you are not using DIH, you can still send a full delete just so the mappings are set, or configure ES index beforehand the same way esURP does.
- for _delete_ operations, just by _id_ or \*:\* are supported.
- after you do the indexing to ES, if you still want to query Solr, it might be better to put the original jars in place, or some component might fail, for instance the ExpandComponent fails in my setup (due to the newer hppc jar from ES).
- Important: ES must be using the same Lucene version Solr is using.

Contributing
----------------

Feel free. Pull requests, issues etc are welcome. 

**Contact**: jmlucjav AT Google's mail

License
----------------

This is released under Apache 2.0 License. 



