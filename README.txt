Place map_2.out and map_7.out in /tmp folder

mvn clean package

hadoop dfs -rmr -skipTrash /tmp/root/staging/job.jar /tmp/*.jar; HADOOP_USER_CLASSPATH_FIRST=true HADOOP_CLASSPATH=/root/tez-1494/target/tez_1494-0.0.1-SNAPSHOT.jar:/root/tez-autobuild/dist/tez/*:/root/tez-autobuild/dist/tez/lib/*:/root/tez-autobuild/dist/tez/conf:$HADOOP_CLASSPATH yarn jar /root/tez-1494/target/tez_1494-0.0.1-SNAPSHOT.jar org.apache.tez.Tez_1494 -Dtez.shuffle-vertex-manager.enable.auto-parallel=true /tmp/map_2.out /tmp/map_7.out /tmp/test.out.1484

You may tweak it to suit your environment

With -Dtez.shuffle-vertex-manager.enable.auto-parallel=false, this job would succeed.  Paralleism changed event is not propagated to Map_5 (which has got both incoming edges as broadcast)
