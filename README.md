# presto-event-listener-plugin

This can be configured to send the presto query create, split computed and the query complete events to different locations.
Currently supported locations are :
1. local disk 
2. kafka 

These events can be used to further analysed to derive insights about the below things and much more : 
1. Kind of queries getting executed in the systems.
2. Query execution time
3. Can be used to infer about the scalability requirement of the presto and further be used to auto up-scale and down-scale presto clusters.
4. Auditing
5. Query Analytics like most frequent used tables & columns, most joined tables, etc.  
