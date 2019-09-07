#!/bin/bash


msgCtHost=$1
msgCtUser=$2
msgCtPwd="g3+{C'0SW,7kd>tA6*}[X\"~]a"
localFile=$3
msgCtDir=$4
exp="/e3base/zhangruiDeWork/messageApp/scp/scp2messageCenter.exp"

echo $msgCtPwd
expect -f $exp $msgCtHost $msgCtUser  $msgCtPwd $localFile $msgCtDir
