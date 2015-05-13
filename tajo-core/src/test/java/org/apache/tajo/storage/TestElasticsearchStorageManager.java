/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.elasticsearch.ElasticsearchStorageManager;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.KeyValueSet;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class TestElasticsearchStorageManager extends QueryTestCaseBase {
  private ImmutableSettings.Builder settings;
  private Node node;
  private Client client;
  private String storeType;
  private TableDesc tableDesc;
  private TableMeta tableMeta;
  private Schema schema;
  private KeyValueSet options;

  public TestElasticsearchStorageManager() {
    this.storeType = "ELASTICSEARCH";
  }

  @Before
  public void setUp() throws Exception {
    tableDesc = new TableDesc();

    settings = ImmutableSettings.settingsBuilder();
    settings.put("cluster.name", "testTajoCluster");
    settings.put("node.name", "testTajoNode");
    settings.put("path.data", "data/index");    // path.data create a relative path which is located in tajo-storage-elasticsearch/data/index
    settings.put("gateway.type", "none");
//    settings.put("index.store.type", "memory");
    settings.put("http.enabled", false);
    settings.put("transport.connections_per_node.recovery", 0);
    settings.put("transport.connections_per_node.bulk", 0);
    settings.put("transport.connections_per_node.reg", 1);
    settings.put("transport.connections_per_node.high", 0);
    settings.put("transport.connections_per_node.ping", 1);

    node = NodeBuilder.nodeBuilder()
        .settings(settings)
        .data(true)
        .local(false)  // Nodes outside of the JVM will not be discovered.
        .node();    // included start() method.

    client = node.client();

    createIndex();
  }

  @After
  public void tearDown() throws Exception {
    client.close();
    node.close();
  }

  @Test
  public void testCreateTable() throws Exception {
    executeString("create external table test_index ( _type text, _score double, _id text, field1 bigint, field2 text, field3 text ) using ELASTICSEARCH with ( 'es.cluster'='testTajoCluster', 'es.nodes'='localhost:9300', 'es.index'='test_index', 'es.type'='test_type', 'es.fetch.size'='100')").close();

    assertTableExists("test_index");

    TajoClient tajoClient = getClient();
    TableDesc desc = tajoClient.getTableDesc("test_index");
    TableStats stats = desc.getStats();

    assertEquals(new Long(1000), stats.getNumRows());
  }

  @Test
  public void testGetNonForwardSplit() throws IOException {
    List<Fragment> fragments;

    options = new KeyValueSet();
    options.set("es.cluster", "testTajoCluster");
    options.set("es.nodes", "localhost:9300");
    options.set("es.index", "test_index");
    options.set("es.type", "test_type");
    options.set("es.fetch.size", "100");

    schema = new Schema();
    schema.addColumn("_type", TajoDataTypes.Type.TEXT);
    schema.addColumn("_score", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("_id", TajoDataTypes.Type.TEXT);
    schema.addColumn("field1", TajoDataTypes.Type.INT8);
    schema.addColumn("field2", TajoDataTypes.Type.TEXT);
    schema.addColumn("field3", TajoDataTypes.Type.TEXT);

    tableMeta = CatalogUtil.newTableMeta(storeType, options);
    tableDesc.setExternal(true);
    tableDesc.setName("test_index");
    tableDesc.setSchema(schema);
    tableDesc.setMeta(tableMeta);

    ElasticsearchStorageManager sm = new ElasticsearchStorageManager(storeType);
    fragments = sm.getNonForwardSplit(tableDesc, 0, 100);

    assertEquals(10, fragments.size());
  }

  @Test
  public void testGetSplits() throws IOException {
    ScanNode scanNode = new ScanNode(1);
    List<Fragment> fragments;

    options = new KeyValueSet();
    options.set("es.cluster", "testTajoCluster");
    options.set("es.nodes", "localhost:9300");
    options.set("es.index", "test_index");
    options.set("es.type", "test_type");
    options.set("es.fetch.size", "100");

    schema = new Schema();
    schema.addColumn("_type", TajoDataTypes.Type.TEXT);
    schema.addColumn("_score", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("_id", TajoDataTypes.Type.TEXT);
    schema.addColumn("field1", TajoDataTypes.Type.INT8);
    schema.addColumn("field2", TajoDataTypes.Type.TEXT);
    schema.addColumn("field3", TajoDataTypes.Type.TEXT);

    tableMeta = CatalogUtil.newTableMeta(storeType, options);
    tableDesc.setExternal(true);
    tableDesc.setName("test_index");
    tableDesc.setSchema(schema);
    tableDesc.setMeta(tableMeta);

    ElasticsearchStorageManager sm = new ElasticsearchStorageManager(storeType);
    fragments = sm.getSplits("elasticsearch_fragments", tableDesc, scanNode);

    assertEquals(10, fragments.size());
  }

  private void createIndex() throws Exception {
    // delete index
    try {
      client.admin().indices().prepareDelete("test_ndex").execute().actionGet();
    } catch (Exception e) {
    } finally {
    }

    // create index
    Settings indexSettings = ImmutableSettings.settingsBuilder()
        .put("number_of_shards","1")
        .put("number_of_replicas", "0")
        .build();

    XContentBuilder builder = XContentFactory.jsonBuilder()
        .startObject()
        .startObject("test-type")
        .startObject("_all")
        .field("enabled", "false")
        .endObject()
        .startObject("_id")
        .field("path", "field1")
        .endObject()
        .startObject("properties")
        .startObject("field1")
        .field("type", "long").field("store", "no").field("index", "not_analyzed")
        .endObject()
        .startObject("field2")
        .field("type", "string").field("store", "no").field("index", "not_analyzed")
        .endObject()
        .startObject("field3")
        .field("type", "string").field("store", "no").field("index", "analyzed")
        .endObject()
        .endObject()
        .endObject()
        .endObject();

    CreateIndexResponse res = client.admin().indices().prepareCreate("test_index")
        .setSettings(indexSettings)
        .addMapping("test_type", builder)
        .execute()
        .actionGet();

    assertEquals(res.isAcknowledged(), true);

    // add document
    IndexRequestBuilder indexRequestBuilder = client.prepareIndex().setIndex("test_index").setType("test_type");
    IndexResponse indexResponse;

    for ( int i=0; i<1000; i++ ) {
      builder = XContentFactory.jsonBuilder()
          .startObject()
          .field("field1", i).field("field2", "henry" + i).field("field3", i + ". hello world!! elasticsearch on apache tajo!!")
          .endObject();

      indexResponse = indexRequestBuilder.setSource(builder)
          .setId(String.valueOf(i))
          .setOperationThreaded(false)
          .setConsistencyLevel(WriteConsistencyLevel.QUORUM)
          .setReplicationType(ReplicationType.ASYNC)
          .execute()
          .actionGet();

      assertEquals(indexResponse.isCreated(), true);
    }

    client.admin().indices().prepareRefresh("test_index").execute().actionGet();
  }
}
