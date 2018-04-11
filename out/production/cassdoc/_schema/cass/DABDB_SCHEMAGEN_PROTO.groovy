package _schema.cass

CassSchemaGen gen = new CassSchemaGen()

String keyspace = "proto_jsonstore"
int replicationFactor = 1

Map entityTypes = [
  "PROD" : [
    ["provider", "text"],
    ["gtin", "text"],
    ["condition", "text"],
    ["sku", "text"],
    ["miid", "text"],
    [
      "submit_date",
      "timestamp"]
  ],
  "JOB" : [
    ["provider", "text"],
    [
      "submit_date",
      "timestamp"]
  ],
  "CJOB" : [
    ["providerUser", "text"],
    ["provider", "text"],
    [
      "submit_date",
      "timestamp"]
  ],
]

println """
DROP KEYSPACE ${keyspace};
CREATE KEYSPACE $keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '$replicationFactor'}  AND durable_writes = true;
"""

println """
CREATE TABLE ${keyspace}.r (
  p1 text,
  ty1 text,
  ty2 text,
  ty3 text,
  ty4 text,
  p2 text,
  p3 text,
  p4 text,
  c1 text,
  c2 text,
  c3 text,
  c4 text,
  d text,
  link text,
  z_md text,  
  
  PRIMARY KEY ((p1),ty1,ty2,ty3,ty4,p2,p3,p4,c1,c2,c3,c4)
);
"""

// i1-3 index type k1-3 key tuple v1-v3 value tuple
println """
CREATE TABLE ${keyspace}.i (
  i1 text,
  i2 text,
  i3 text,
  k1 text,
  k2 text,
  k3 text,
  v1 text,
  v2 text,
  v3 text,
  id text,
  d text,  
  PRIMARY KEY ((i1,i2,i3,k1,k2,k3),v1,v2,v3)
);
"""


for (String entSuff : entityTypes.keySet()) {
  println gen.entityTable(keyspace,entSuff,entityTypes[entSuff])
  println gen.attrTable(keyspace,entSuff)
}

println """


CREATE MATERIALIZED VIEW proto_jsonstore.e_PROD_sku AS
SELECT sku, e FROM proto_jsonstore.e_PROD
  WHERE sku is NOT NULL AND e is NOT NULL
  PRIMARY KEY (sku,e);    

"""