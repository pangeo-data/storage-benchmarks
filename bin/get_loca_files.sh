#!/bin/bash
# script to download LOCA files
#
# TBD: enable specified years to be downloaded
if [ $# -eq 0 ] || ([ $1 == "-h" ] || [ $1 == "--help" ]); then
   echo "Usage: get_loca_files.sh [target_dir] [yearstart] [yearend]"
   echo "If yearstart is not provided, starting year will be 1950."
   echo "If yearstartis provided, but not yearend, ending year will be 2005."
   echo "Approx 20GB of diskspace will be needed to store entire collection"
   exit 1
fi 
startyear=1950
endyear=2005
if [ $# -gt 1 ]; then
   startyear=$2
   startyear=$(($startyear + 0))
fi
if [ $# -gt 2 ]; then
   endyear=$3
   endyear=$(($endyear + 0))
fi

echo "start:  ($startyear + 1)"
echo "end: " $endyear
for i in {$startyear..$endyear}
do
   echo "Year $i "
done

LOCA_URL="http://nasanex.s3.amazonaws.com/LOCA/ACCESS1-0/16th/historical/r1i1p1/tasmax"
LOCA_NAME="tasmax_day_ACCESS1-0_historical_r1i1p1_19500101-19501231.LOCA_2016-04-02.16th.nc"
TARGET=$1
# wget the file
#wget --directory-prefix=${TARGET} ${LOCA_URL}/${LOCA_NAME}


