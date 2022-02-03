#! /bin/bash
#measure memory usage, dosk read/write in MB/s, and CPU usage as well as cpu io wait
MEMTOTAL=$(grep -i memtotal /proc/meminfo)
CPUCNT=$(getconf _NPROCESSORS_ONLN)
#assuming all disks have same blocksize as current disk
BLCKSIZE=$(stat -fc %s ./)

OLD_DISK_READ=$(cat /proc/diskstats|awk 'BEGIN{sum=0}{sum +=$4}END{print sum}')
OLD_DISK_WRITE=$(cat /proc/diskstats|awk 'BEGIN{sum=0}{sum +=$8}END{print sum}')
OLD_CPU_TOTAL=$(cat /proc/stat|grep '^cpu'|head -n1 |tr -s " "|awk '{sum=0;for(i=2;i<=NF;i++){sum+=$i}print sum}')
OLD_CPU_USED=$(cat /proc/stat|grep '^cpu'|head -n1 |tr -s " "|awk '{sum=$2+$3+$4;print sum}')
OLD_CPU_IO=$(cat /proc/stat|grep '^cpu'|head -n1 |tr -s " "|awk '{print $6}')
DELAY=5
echo "#CPU count: $CPUCNT"
echo "#IO usage in MB/s"
echo "#Memory usage in GB"
printf '%-s\t%-s\t%-s\t%-s\t%-s\t%-s\t%-s\t%-s\n' "Time" "Memtotal" "MemUsed" "DiskFree" "DiskR" "DiskW"  "CPU" "CPUWaitIO"
while :
do
  NOW=$(date +%s)
  #timestamp
  DATE=$(date +'%F %T')
  #memory usage
  MEMTOTAL=$(free -m | awk 'NR==2{printf "%.2f", $2/1024}')
  MEMUSED=$(free -m | awk 'NR==2{printf "%.2f", $3/1024 }')
  #current CPU cycle numbers
  CPU_TOTAL=$(cat /proc/stat|grep '^cpu'|head -n1 |tr -s " "|awk '{sum=0;for(i=2;i<=NF;i++){sum+=$i}print sum}')
  CPU_USED=$(cat /proc/stat|grep '^cpu'|head -n1 |tr -s " "|awk '{sum=$2+$3+$4;print sum}')
  CPU_IO=$(cat /proc/stat|grep '^cpu'|head -n1 |tr -s " "|awk '{print $6}')
  #CPU change since last measurement
  CPU_USED_CHANGE=$(expr $CPU_USED - $OLD_CPU_USED)
  CPU_TOTAL_CHANGE=$(expr $CPU_TOTAL - $OLD_CPU_TOTAL)
  CPU_IO_CHANGE=$(expr $CPU_IO - $OLD_CPU_IO)
  #CPu numbers
  CPU=$(awk -v t=$CPU_TOTAL_CHANGE -v u=$CPU_USED_CHANGE 'BEGIN{printf "%.2f", u*100/t}')
  CPUIO=$(awk -v t=$CPU_TOTAL_CHANGE -v u=$CPU_IO_CHANGE 'BEGIN{printf "%.2f", u*100/t}')
  #current disk reads
  NEW_DISK_READ=$(cat /proc/diskstats|awk 'BEGIN{sum=0}{sum +=$4}END{print sum}')
  NEW_DISK_WRITE=$(cat /proc/diskstats|awk 'BEGIN{sum=0}{sum +=$8}END{print sum}')
  #disk usage in MB/s
  DISKR=$(awk -v d=$DELAY -v n=$NEW_DISK_READ -v o=$OLD_DISK_READ -v b=$BLCKSIZE 'BEGIN{printf "%.2f", b*(n-o)/(1024*1024*d)}')
  DISKW=$(awk -v d=$DELAY -v n=$NEW_DISK_WRITE -v o=$OLD_DISK_WRITE -v b=$BLCKSIZE 'BEGIN{printf "%.2f", b*(n-o)/(1024*1024*d)}')
  
  DISKFREE=$(df -h |grep "/cromwell_root"|tr -s " "|awk '{print $4}')
  #update old values
  OLD_DISK_READ=$NEW_DISK_READ
  OLD_DISK_WRITE=$NEW_DISK_WRITE
  OLD_CPU_TOTAL=$CPU_TOTAL
  OLD_CPU_USED=$CPU_USED
  OLD_CPU_IO=$CPU_IO
  printf '%-s\t%-s\t%-s\t%-s\t%-s\t%-s\t%s%%\t%s%%\n' "$DATE" "$MEMTOTAL" "$MEMUSED" "$DISKFREE" "$DISKR" "$DISKW" "$CPU" "$CPUIO"
  #sleep until measurement start + DELAY, so that measurement drift is not so bad
  #kinda overkill, but eh
  NOW_2=$(date +%s)
  SLEEP_UNTIL_=$(expr $NOW - $NOW_2 + $DELAY)
  SLEEP_UNTIL=$((SLEEP_UNTIL_ < 0 ? 0 : SLEEP_UNTIL_))
  sleep $SLEEP_UNTIL
done
