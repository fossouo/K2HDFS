JDK 1.8 

Scala 2.11 for Intellij 

Maven plugin in Intellij 

Run from project directory in Shell :


[hdfs@c225-node2 tmp]$ spark-submit --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 2g --num-executors 4 --files /tmp/props2 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 kafka-to-hdfs-1.0-jar-with-dependencies.jar ./props2

Expected output :

19/08/06 10:27:09 INFO StreamExecution: Streaming query made progress: {
  "id" : "30150aea-4f3f-4c28-ae7d-379f875d47c6",
  "runId" : "8ba94e8d-017f-430f-b2cc-378c8dea8ce7",
  "name" : null,
  "timestamp" : "2019-08-06T10:27:05.000Z",
  "numInputRows" : 6440,
  "inputRowsPerSecond" : 1288.0,
  "processedRowsPerSecond" : 1369.6299447043812,
  "durationMs" : {
    "addBatch" : 4043,
    "getBatch" : 26,
    "getOffset" : 2,
    "queryPlanning" : 202,
    "triggerExecution" : 4702,
    "walCommit" : 414
  },
  "stateOperators" : [ ],
  "sources" : [ {
    "description" : "KafkaSource[Subscribe[test2]]",
    "startOffset" : {
      "test2" : {
        "0" : 63502966
      }
    },
    "endOffset" : {
      "test2" : {
        "0" : 63509406
      }
    },
    "numInputRows" : 6440,
    "inputRowsPerSecond" : 1288.0,
    "processedRowsPerSecond" : 1369.6299447043812
  } ],
  "sink" : {
    "description" : "FileSink[/tmp/hive_table]"
  }
}