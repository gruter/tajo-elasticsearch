# Apache Tajo & Elasticsearch
* Collaborate Apache Tajo + Elasticsearch

# Apache Tajo User Group
* [Go to Link](https://groups.google.com/forum/#!forum/tajo-user-kr)

# Apache Tajo Mailing List
* [Go to Link](http://tajo.apache.org/mailing-lists.html)

# Elasticsearch User Group
* [Go to Link](https://www.facebook.com/groups/elasticsearch.kr/)
* [Go to Link](https://groups.google.com/forum/#!forum/elasticsearch)

# Registerer
* hwjeong@gruter.com

# Master Branch Build Environment
* JDK 6 or later
* Elasticsearch 1.1.2

# tajo-es-1.5.2 Branch Build Environment
* JDK 7 or later
* Elasticsearch 1.5.2

# tajo-es-1.1.2 Branch Build Environment
* JDK 6 or later
* Elasticsearch 1.1.2

# HADOOP
```
$ cd ~/server/app/hadoop-2.3.0
```

# Prerequisites
* Hadoop 2.3.0 or higher (up to 2.5.1)
* Java 1.6 or 1.7
* Protocol buffer 2.5.0
* [Go to Link](http://tajo.apache.org/docs/0.10.0/getting_started.html)

# Source Clone & Build
```
$ git clone https://github.com/gruter/tajo-elasticsearch.git
$ cd tajo-elasticsearch
$ mvn clean package -DskipTests -Pdist -Dtar
$ cd tajo-dist/target/
$ ls -al tajo-0.*.tar.gz
-rw-r--r--  1 hwjeong  staff  59521544  5 14 13:59 tajo-0.11.0-SNAPSHOT.tar.gz
```

# Apache Tajo Installation
* [Go to Link](http://tajo.apache.org/docs/0.10.0/getting_started.html)
```
$ cd ~/server/app
$ mkdir tajo
$ cd tajo
$ cp ~/git/tajo-elasticsearch/tajo-dist/target/tajo-0.11.0-SNAPSHOT.tar.gz .
$ tar -xvzf tajo-0.11.0-SNAPSHOT.tar.gz
$ ln -s tajo-0.11.0-SNAPSHOT tajo
$ cd tajo
$ vi conf/tajo-env.sh
export HADOOP_HOME=/Users/hwjeong/server/app/hadoop-2.3.0
export JAVA_HOME=`/usr/libexec/java_home -v 1.7`
```

# Apache Tajo Worker - ssh keygen
```
$ cd ~/.ssh
$ ssh-keygen -t rsa
$ cat id_rsa.pub > authorized_keys
$ chmod 600 authorized_keys
```

# Apache Tajo Run & Sample Data
```
$ cd ~/server/app/tajo/tajo
$ bin/start-tajo.sh
$ cat > data.csv
1|abc|1.1|a
2|def|2.3|b
3|ghi|3.4|c
4|jkl|4.5|d
5|mno|5.6|e
^C
```

# Hadoop Run & Make User Directory
```
$ cd ~/server/app/hadoop-2.3.0
$ sbin/start-all.sh
$ bin/hadoop fs -ls /
$ bin/hadoop fs -mkdir /user/tajo
$ bin/hadoop fs -chown hwjeong /user/tajo
$ bin/hadoop fs -ls /user
drwxr-xr-x   - hwjeong supergroup          0 2015-05-14 14:42 /user/tajo
$ bin/hadoop fs -moveFromLocal ~/server/app/tajo/tajo/data.csv /user/tajo/
```

# Apache Tajo CLI
```
$ cd ~/server/app/tajo/tajo
$ bin/sql
default> create external table tajodemotbl (id int, name text, score float, type text) using csv with ('csvfile.delimiter'='|') location 'hdfs://localhost:9000/user/tajo/data.csv';
OK
default> \d tajodemotbl;

table name: default.tajodemotbl
table path: hdfs://localhost:9000/user/tajo/data.csv
store type: csv
number of rows: unknown
volume: 60 B
Options:
'text.delimiter'='|'

schema:
id	INT4
name	TEXT
score	FLOAT4
type	TEXT

default> select * from tajodemotbl where id > 2;
Progress: 0%, response time: 1.557 sec
Progress: 0%, response time: 1.558 sec
Progress: 100%, response time: 1.86 sec
id,  name,  score,  type
-------------------------------
3,  ghi,  3.4,  c
4,  jkl,  4.5,  d
5,  mno,  5.6,  e
(3 rows, 1.86 sec, 48 B selected)
default>
```

# Elasticsearch Installation & Run
* [Go to Link](http://www.elastic.co/guide/en/elasticsearch/guide/master/_installing_elasticsearch.html)
* Installed elasticsearch-1.1.2
```
$ cd ~/server/app/elasticsearch/elasticsearch-1.1.2
# no configuration
$ bin/elasticsearch -f
```

# Create Index & Document
```java
package org.gruter.elasticsearch.test;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class ElasticsearchCRUDTest {
  private static final Logger log = LoggerFactory.getLogger(ElasticsearchCRUDTest.class);

  private ImmutableSettings.Builder settings;
  private Node node;
  private Client client;
  private int DOC_SIZE = 1000;

  @Before
  public void setup() throws Exception {
    settings = ImmutableSettings.settingsBuilder();
    settings.put("cluster.name", "elasticsearch");

    node = NodeBuilder.nodeBuilder()
        .settings(settings)
        .data(false)
        .local(false)
        .client(true)
        .node();

    client = node.client();
  }

  @Test
  public void testElasticsearchCRUD() throws Exception {
    // delete index
    try {
      client.admin().indices().prepareDelete("tajo_es_index").execute().actionGet();
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
          .startObject("tajo_es_type")
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

    CreateIndexResponse res = client.admin().indices().prepareCreate("tajo_es_index")
        .setSettings(indexSettings)
        .addMapping("tajo_es_type", builder)
        .execute()
        .actionGet();

    assertEquals(res.isAcknowledged(), true);

    // add document
    IndexRequestBuilder indexRequestBuilder = client.prepareIndex().setIndex("tajo_es_index").setType("tajo_es_type");
    IndexResponse indexResponse;

    for ( int i=0; i<DOC_SIZE; i++ ) {
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

    client.admin().indices().prepareRefresh("tajo_es_index").execute().actionGet();

    CountRequest request = new CountRequest();
    request.indices("tajo_es_index").types("tajo_es_type");
    CountResponse response = client.count(request).actionGet();
    long count = response.getCount();

    assertEquals(count, DOC_SIZE);
  }

  @After
  public void tearDown() throws Exception {
    client.close();
    node.close();
  }
}
```

# Check Status
* [Go to Link](http://localhost:9200/_status?pretty=true)

# Create External Table for Elasticsearch on Tajo and Test Query
```
create external table tajo_es_index (
  _type text,
  _score double,
  _id text,
  field1 bigint,
  field2 text,
  field3 text
)
using elasticsearch
with (
  'es.index'='tajo_es_index',
  'es.type'='tajo_es_type'
)

$ cd ~/server/app/tajo/tajo
$ bin/tsql

Try \? for help.
default> create external table tajo_es_index (
>   _type text,
>   _score double,
>   _id text,
>   field1 bigint,
>   field2 text,
>   field3 text
> )
> using elasticsearch
> with (
>   'es.index'='tajo_es_index',
>   'es.type'='tajo_es_type'
> );
OK
default> select count(*) from tajo_es_index;
Progress: 0%, response time: 1.397 sec
Progress: 0%, response time: 1.398 sec
Progress: 0%, response time: 1.802 sec
Progress: 100%, response time: 1.808 sec
?count
-------------------------------
1000
(1 rows, 1.808 sec, 5 B selected)

default> select * from tajo_es_index where field1 > 10 and field1 < 15;
Progress: 100%, response time: 0.583 sec
_type,  _score,  _id,  field1,  field2,  field3
-------------------------------
tajo_es_type,  0.0,  11,  11,  henry11,  11. hello world!! elasticsearch on apache tajo!!
tajo_es_type,  0.0,  12,  12,  henry12,  12. hello world!! elasticsearch on apache tajo!!
tajo_es_type,  0.0,  13,  13,  henry13,  13. hello world!! elasticsearch on apache tajo!!
tajo_es_type,  0.0,  14,  14,  henry14,  14. hello world!! elasticsearch on apache tajo!!
(4 rows, 0.583 sec, 320 B selected)
```

# Elasticsearch "with" Options
```java
  public static final String OPT_CLUSTER = "es.cluster";
  public static final String OPT_NODES = "es.nodes";
  public static final String OPT_INDEX = "es.index";
  public static final String OPT_TYPE = "es.type";
  public static final String OPT_FETCH_SIZE = "es.fetch.size";
  public static final String OPT_PRIMARY_SHARD = "es.primary.shard";
  public static final String OPT_REPLICA_SHARD = "es.replica.shard";
  public static final String OPT_PING_TIMEOUT = "es.ping.timeout";
  public static final String OPT_CONNECT_TIMEOUT = "es.connect.timeout";
  public static final String OPT_THREADPOOL_RECOVERY = "es.threadpool.recovery";
  public static final String OPT_THREADPOOL_BULK = "es.threadpool.bulk";
  public static final String OPT_THREADPOOL_REG = "es.threadpool.reg";
  public static final String OPT_TIME_SCROLL = "es.time.scroll";
  public static final String OPT_TIME_ACTION = "es.time.action";
```