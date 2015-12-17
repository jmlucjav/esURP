package com.jmlucjav.esURP;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class ESClientHelper {
    private final static Logger log = LoggerFactory.getLogger(ESClientHelper.class);


    public static Client getESClient(String esCluster, boolean useTransportClient) {
        Client client;
        if (useTransportClient) {
            InetAddress addr = null;
            try {
                addr = InetAddress.getByName("127.0.0.1");
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            TransportClient tclient = TransportClient.builder().settings(Settings.builder()
                    .put("cluster.name", esCluster)
                    .put("client.transport.sniff", true)
                    .build()).build();
            tclient.addTransportAddress(new InetSocketTransportAddress(addr, 9300));
            client = tclient;
        } else {
            // We use a Node because the connection to ES cluster will be hold during long time (imagine indexing million docs)
            // Embedded node clients behave just like standalone nodes, which means that they will leave the HTTP port open !
            Node node = nodeBuilder().clusterName(esCluster)
                    //I had to use this settings so I could join ES in my localhost
                    .settings(Settings.settingsBuilder().put("http.enabled", false)
                    .put("discovery.zen.ping.multicast.enabled", "false")
                    .put("discovery.zen.ping.unicast.hosts", "localhost"))
                    .client(true)
                    .node();
            client = node.client();
        }
        return client;
    }

    public static void closeClient(Client client) {
        client.close();
    }
}
