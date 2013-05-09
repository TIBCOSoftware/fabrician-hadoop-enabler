# Set Hadoop-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
# export JAVA_HOME=/usr/lib/j2sdk1.5-sun

# Extra Java CLASSPATH elements.  Optional.
# export HADOOP_CLASSPATH=

# The maximum amount of heap to use, in MB. Default is 1000.
# export HADOOP_HEAPSIZE=2000

# Extra Java runtime options.  Empty by default.
# export HADOOP_OPTS=-server

# Command specific options appended to HADOOP_OPTS when specified
export HADOOP_NAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_NAMENODE_OPTS"
export HADOOP_SECONDARYNAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_SECONDARYNAMENODE_OPTS"
export HADOOP_DATANODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_DATANODE_OPTS"
export HADOOP_BALANCER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_BALANCER_OPTS"
export HADOOP_JOBTRACKER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_JOBTRACKER_OPTS"
# export HADOOP_TASKTRACKER_OPTS=
# The following applies to multiple commands (fs, dfs, fsck, distcp etc)
# export HADOOP_CLIENT_OPTS

# Extra ssh options.  Empty by default.
# export HADOOP_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=HADOOP_CONF_DIR"

# Where log files are stored.  $HADOOP_HOME/logs by default.
# export HADOOP_LOG_DIR=${HADOOP_HOME}/logs

# File naming remote slave hosts.  $HADOOP_HOME/conf/slaves by default.
# export HADOOP_SLAVES=${HADOOP_HOME}/conf/slaves

# host:path where hadoop code should be rsync'd from.  Unset by default.
# export HADOOP_MASTER=master:/home/$USER/src/hadoop

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export HADOOP_SLAVE_SLEEP=0.1

# The directory where pid files are stored. /tmp by default.
# export HADOOP_PID_DIR=/var/hadoop/pids

# A string representing this instance of hadoop. $USER by default.
# export HADOOP_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export HADOOP_NICENESS=10

####################################################
# Silver Fabric Enabler Modifications
# 
# The following lines required by the Silver
# Fabric Hadoop enabler
#
###################################################
export JAVA_HOME=${GRIDLIB_JAVA_HOME}

export JMX_OPTS=" -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.port"
#    -Dcom.sun.management.jmxremote.password.file=$HADOOP_HOME/conf/jmxremote.password \
#    -Dcom.sun.management.jmxremote.access.file=$HADOOP_HOME/conf/jmxremote.access"
export HADOOP_NAMENODE_OPTS="$JMX_OPTS=${hadoop_enabler_NAMENODE_JMX_BASEPORT} $HADOOP_NAMENODE_OPTS"
export HADOOP_SECONDARYNAMENODE_OPTS="$JMX_OPTS=${hadoop_enabler_SECONDARYNAMENODE_JMX_BASEPORT} $HADOOP_SECONDARYNAMENODE_OPTS"
export HADOOP_DATANODE_OPTS="$JMX_OPTS=${hadoop_enabler_DATANODE_JMX_BASEPORT} $HADOOP_DATANODE_OPTS"
export HADOOP_BALANCER_OPTS="$HADOOP_BALANCER_OPTS"
export HADOOP_JOBTRACKER_OPTS="$JMX_OPTS=${hadoop_enabler_JOBTRACKER_JMX_BASEPORT} $HADOOP_JOBTRACKER_OPTS"
export HADOOP_TASKTRACKER_OPTS="$JMX_OPTS=${hadoop_enabler_TASKTRACKER_JMX_BASEPORT} $HADOOP_TASKTRACKER_OPTS"

export HADOOP_SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o PasswordAuthentication=no"
export HADOOP_LOG_DIR=${hadoop_enabler_HADOOP_HOME_DIR}/logs
export HADOOP_PID_DIR=${hadoop_enabler_TMP_DIR}

# Added $HIVE_OPTS that is set by hive-env.sh when starting hiveserver
export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true $HADOOP_CLIENT_OPTS $HIVE_OPTS"

