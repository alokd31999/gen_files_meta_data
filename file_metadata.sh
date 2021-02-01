#!/bin/bash
echo $(whoami)
export PYSPARK_DRIVER_PYTHON=python
export PYSPARK_PYTHON=python
export TZ=EST
TIMESTAMP="$(date '+%Y%m%d%H%M%S')"
# making dir if not exist 
mkdir -p /app/logs
download_files_log="/app/logs/download_files_log.txt"
gen_file_meta_data_log="/app/logs/gen_file_meta_data_log.txt"
success_or_failure="/app/logs/success_or_failure.log"

if [ -e $success_or_failure ]
then 
cat /dev/null > $success_or_failure
fi 


export BASEDIR=/app
cd $BASEDIR
echo "$(date '+%Y-%m-%d %H:%M:%S')-----start file_meta_data process"
_all_jobs() {
    # ---------------------------------------------------------------------------------------------------------------
    # Download files
    # ---------------------------------------------------------------------------------------------------------------
    "_dwonload_files"                                                                                               \
    # ---------------------------------------------------------------------------------------------------------------
    # ---------------------------------------------------------------------------------------------------------------
    # Generate file_meta_data
    # ---------------------------------------------------------------------------------------------------------------
    "_gen_file_meta_data"                                                                                            \
    # ---------------------------------------------------------------------------------------------------------------
    }

_dwonload_files() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') start dwonload_files  dwonload_files.py "
  python download_files.py > $download_files_log 2>&1
  retcode=`echo $?`
  case "$retcode" in
  0) echo "$(date '+%Y-%m-%d %H:%M:%S')----- dwonload_files  dwonload_files.py execution successful" ;;
  1) echo "$(date '+%Y-%m-%d %H:%M:%S')----- dwonload_files  dwonload_files.py exit with failure" ;;
  2) echo "$(date '+%Y-%m-%d %H:%M:%S')----- dwonload_files  dwonload_files.py exit with warning" ;;
  *) echo "$(date '+%Y-%m-%d %H:%M:%S')----- dwonload_files  dwonload_files.py Unknow return code" ;;
  esac
  if [ $retcode -ne 0 ]
  then
  echo "$(date '+%Y-%m-%d %H:%M:%S')-----dwonload_files dwonload_files.py with failure code: $retcode, investigate logs, code and then restart"
  echo "$timestamp dwonload_files failure " >> $success_or_failure 
  exit $retcode
  else
  echo "$(date '+%Y-%m-%d %H:%M:%S')-----dwonload_files dwonload_files.py with success code: $retcode "
  echo "$timestamp dwonload_files success " >> $success_or_failure 
  fi
}
_gen_file_meta_data() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') start gen_file_meta_data  gen_file_meta_data.py"
  #spark-submit --master yarn --driver-memory 1G --executor-memory 1G --executor-cores 1  --num-executors 1  gen_file_meta_data.py
  spark-submit --master local[1] --driver-memory 1G --executor-memory 1G --executor-cores 1  --num-executors 1  gen_file_meta_data.py > $gen_file_meta_data_log 2>&1
  retcode=`echo $?`
  case "$retcode" in
  0) echo "$(date '+%Y-%m-%d %H:%M:%S')-----gen_file_meta_data gen_file_meta_data.py execution successful" ;;
  1) echo "$(date '+%Y-%m-%d %H:%M:%S')-----gen_file_meta_data gen_file_meta_data.py exit with failure" ;;
  2) echo "$(date '+%Y-%m-%d %H:%M:%S')-----gen_file_meta_data gen_file_meta_data.py exit with warning" ;;
  *) echo "$(date '+%Y-%m-%d %H:%M:%S')-----gen_file_meta_data gen_file_meta_data.py Unknow return code" ;;
  esac
  if [ $retcode -ne 0 ]
  then
  echo "$(date '+%Y-%m-%d %H:%M:%S')-----gen_file_meta_data gen_file_meta_data.py with failure code: $retcode, investigate logs, code and then restart"
  echo "$timestamp gen_file_meta_data failure " >> $success_or_failure 
  exit $retcode
  else
  echo "$(date '+%Y-%m-%d %H:%M:%S')-----gen_file_meta_data gen_file_meta_data.py with success code: $retcode "
  echo "$timestamp gen_file_meta_data success " >> $success_or_failure 
  fi
}
_all_jobs
echo "$(date '+%Y-%m-%d %H:%M:%S')-----end file_meta_data process"