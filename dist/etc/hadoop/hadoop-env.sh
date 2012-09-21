JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/
this="${BASH_SOURCE-$0}"
conf_dir=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
hadoop_base="${conf_dir}/../../"
hadoop_base=$(cd -P -- "${hadoop_base}" && pwd -P)
#HADOOP_CLASSPATH="${hadoop_base}/../classes"
#HADOOP_CLASSPATH=$(cd -P -- "${HADOOP_CLASSPATH}" && pwd -P)
HADOOP_HOME=$hadoop_base
YARN_HOME=$HADOOP_HOME
HADOOP_COMMON_HOME=$HADOOP_HOME
HADOOP_HDFS_HOME=$HADOOP_HOME
export JAVA_HOME
export YARN_HOME
export HADOOP_HOME
export HADOOP_COMMON_HOME
export HADOOP_HDFS_HOME

HADOOP_CLASSPATH=${hadoop_base}/magicube/magicube.jar
export HADOOP_CLASSPATH

YARN_APPLICATION_CLASSPATH=${HADOOP_CLASSPATH}
export YARN_APPLICATION_CLASSPATH
export MALLOC_ARENA_MAX=4
export HADOOP_HEAPSIZE=2500MB
