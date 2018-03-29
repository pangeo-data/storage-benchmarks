#!/bin/bash
# script to download LOCA files
#
# TBD: enable specified years to be downloaded
if [ $# -eq 0 ] || ([ $1 == "-h" ] || [ $1 == "--help" ]); then
   echo "Usage: get_loca_files.sh [target_dir]"
   exit 1
fi 
LOCA_URL="http://nasanex.s3.amazonaws.com/LOCA/ACCESS1-0/16th/historical/r1i1p1/tasmax"
LOCA_NAME="tasmax_day_ACCESS1-0_historical_r1i1p1_19500101-19501231.LOCA_2016-04-02.16th.nc"
TARGET=$1
# wget the file
wget --directory-prefix=${TARGET} ${LOCA_URL}/${LOCA_NAME}


