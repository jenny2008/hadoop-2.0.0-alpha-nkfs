this="${BASH_SOURCE-$0}"
conf_dir=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
hadoop_base="${conf_dir}/../../"
hadoop_base=$(cd -P -- "${hadoop_base}" && pwd -P)
HADOOP_CLASSPATH="${hadoop_base}/../classes"
HADOOP_CLASSPATH=$(cd -P -- "${HADOOP_CLASSPATH}" && pwd -P)
#echo "HADOOP_CLASSPATH=$HADOOP_CLASSPATH"
