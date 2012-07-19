JAVA_HOME=/opt/sun-jdk-1.6.0.31
this="${BASH_SOURCE-$0}"
conf_dir=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
hadoop_base="${conf_dir}/../../"
hadoop_base=$(cd -P -- "${hadoop_base}" && pwd -P)
HADOOP_CLASSPATH="${hadoop_base}/../classes"
HADOOP_CLASSPATH=$(cd -P -- "${HADOOP_CLASSPATH}" && pwd -P)
HADOOP_HOME=$hadoop_base
YARN_HOME=$HADOOP_HOME
HADOOP_COMMON_HOME=$HADOOP_HOME
HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME
export HADOOP_HOME
export HADOOP_COMMON_HOME
export HADOOP_HDFS_HOME


echo "HADOOP_COMMON_HOME = $HADOOP_COMMON_HOME"
echo "HADOOP_HOME = $HADOOP_HOME"
echo "YARN_HOME = $YARN_HOME"
#echo "HADOOP_CLASSPATH=$HADOOP_CLASSPATH"


