usage="Usage: slaves.sh [--config <conf-dir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

HOSTLIST=`cat "/home/ec2-user/slaves"`

# By default disable strict host key checking
if [ "$SPARK_SSH_OPTS" = "" ]; then
  SPARK_SSH_OPTS="-o StrictHostKeyChecking=no -i /home/ec2-user/.ssh/pipelines-ec2.pem"
fi

for slave in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  if [ -n "${SPARK_SSH_FOREGROUND}" ]; then
    ssh $SPARK_SSH_OPTS "$slave" $"${@// /\\ }" \
      2>&1 | sed "s/^/$slave: /"
  else
    ssh $SPARK_SSH_OPTS "$slave" $"${@// /\\ }" \
      2>&1 | sed "s/^/$slave: /" &
  fi
  if [ "$SPARK_SLAVE_SLEEP" != "" ]; then
    sleep $SPARK_SLAVE_SLEEP
  fi
done

wait

