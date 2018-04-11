# CassDoc

https://docs.google.com/presentation/d/1kHR_P4xBst-6b9hTw7JtDInbiIOxLX8ov3trqXHzH1c/edit?usp=sharing

A JSON Document Storage Engine for Cassandra (Stage 1)

or is it a Document Graph Query Engine? (Stage 2)

or maybe a JCR/CMIS repository engine... or something else. 

or add Dynamo and Postres and Scylla support. CockroachDB?

The core mission is the JSON store.

Secondary missions are the JCR and Tinkerpop3 interfaces. However those have been very useful for teaching me how people might use the core API. As such I've added:

- an overlay-style update attribute api, which enables update of an attribute with mixed existing child document references and new child document references. 
- JsonPath features
- Since JSON seamlessly serializes / deserializes to Maps / Lists / BigInts / BigDecs / booleans / strings, almost all the API calls that you can do with JSON you can do with simple java object trees of maps/lists/bigint/bigdec/bool/str. 
- streaming: I want the ability for really large lists of documents to "streaming insert" without having to waypoint the entire request in the JVM heap of the CassDoc engine. Likewise for large document pulls. 
- paxos: enable a mechanism and process to do paxos-guarded attribute updates (to a degree). Using metadata retrievals, you can get the zv version timeuuid for an attribute and then do a PAXOS-guarded conditional update for some degree of PAXOS transactional operations on document fields/keys/attributes.

Questions: gmail carlemueller
