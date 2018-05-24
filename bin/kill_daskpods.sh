#!/bin/bash
# Brute force clean up ophaned Dask pods 
#
platform=`uname`
OPTIND=1
USER=$JUPYTERHUB_USER

if [[ "$platform" == 'Linux' ]]; then
    KUBECTL='/usr/bin/kubectl'
    user=$USER
elif [[ "$platform" == 'Darwin' ]]; then
    KUBECTL='/usr/local/bin/kubectl'
    user=`id -u`
fi

for pod in $($KUBECTL get po -a | grep dask-${USER}.*Completed | awk '{print $1}')
do
	$KUBECTL delete pod $pod 
done


