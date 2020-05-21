# DART: De-Anonymization of Personal Gazetteers through Social Trajectories

The interest towards trajectory data has sensibly increased 
since the widespread of mobile devices. Simple clustering 
techniques allow the recognition of personal gazetteers, 
i.e., the set of main points of interest (also called stay points) 
of each user, together with the list of time instants of each visit. 
Due to their sensitiveness, personal gazetteers are usually 
anonymized, but their inherent unique patterns expose them 
to the risk of being de-anonymized. In particular, social 
trajectories (i.e., those obtained from social networks, 
which associate statuses and check-ins to spatial and 
temporal locations) can be leveraged by an adversary to 
de-anonymize personal gazetteers. 

DART is proposed as an innovative approach to effectively 
de-anonymize personal gazetteers through social trajectories, 
even in absence of a temporal alignment between the two sources 
(i.e., they have been collected over different time periods). 
DART relies on a big-data implementation, guaranteeing the 
scalability to large volumes of real-word datasets.

This implementation is based on Apache Spark 2.4 and GeoSpark 1.2.0;
tables are stored in Apache Hive.

## Instructions

- The algorithms' parameters are configurable in object ```DartConf```.
- Database's and tables' names must be set in object ```DB```.

The input tables must contain the following fields:

- ```input_table_staypoints``` contains the list of stay points 
for every anonymized trajectory
  - staypointid: String (id of the stay point) 
  - userid: String (id of the corresponding user)
  - latitude: Double (EPSG:4326)
  - longitude: Double (EPSG:4326)
- ```input_table_staypoints_points``` contains the list of every GPS ping,
associated to the stay point of a certain user
  - staypointid: String (id of the stay point) 
  - userid: String (id of the corresponding user)
  - timest: Double (timestamp in seconds)
- ```input_table_knownTrajectories``` contains the list of point
for every known trajectory (e.g., the list of tweets)
  - tweetid: String (id of the point, e.g., the tweet's id) 
  - tweetuserid: String (id of the corresponding user)
  - latitude: Double (EPSG:4326)
  - longitude: Double (EPSG:4326)
  - timest: Double (timestamp in seconds)

The reference configuration of Spark for DART's execution consists of 
10 executors with 3 cores and 10GB of RAM for each executor.

```
spark-submit \
--class it.unibo.big.dart.DartMain \
--master yarn \
--deploy-mode cluster \
--num-executors 10 \
--executor-cores 3 \
--executor-memory 10G \
BIG-DART.jar
```

## Authors

Matteo Francia, Enrico Gallinucci, Matteo Golfarelli, Nicola Santolini

Business Intelligence Group - Department of Computer Science and Engineering - University of Bologna (Cesena, Italy)