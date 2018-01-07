#! /bin/sh

SERVICE_NAME=python-json-suricata-to-elk
PATH=/bin:/usr/bin:/sbin:/usr/sbin
DAEMON=/opt/python-json-suricata-to-elk.sh
PIDFILE=/var/run/scriptname.pid

test -x $DAEMON || exit 0

. /lib/lsb/init-functions

case "$1" in
  start)
     log_daemon_msg "Starting $SERVICE_NAME"
     start_daemon -p $PIDFILE $DAEMON
     log_end_msg $?
   ;;
  stop)
     log_daemon_msg "Stopping $SERVICE_NAME"
     killproc -p $PIDFILE $DAEMON
     PID=`ps x |grep feed | head -1 | awk '{print $1}'`
     kill -9 $PID       
     log_end_msg $?
   ;;
  force-reload|restart)
     $0 stop
     $0 start
   ;;
  status)
     status_of_proc -p $PIDFILE $DAEMON atd && exit 0 || exit $?
   ;;
 *)
   echo "Usage: /etc/init.d/atd {start|stop|restart|force-reload|status}"
   exit 1
  ;;
esac

exit 0