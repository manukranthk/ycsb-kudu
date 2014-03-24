#!/bin/bash
# This script will parse out the overall runtime, throughput and the specified
# operation's average, 95th percentile, and 99th percentile latency.
# usage:
# parse-ycsb.sh operation path_to_ycsb_output
FILE=$2

parse_out() {
  local OP=$1
  local METRIC=$2
  grep "^\[$OP\]\,.*$METRIC" "$FILE" | {
    value=`awk -F , '{print $NF}' | bc | sed 's/[.].*//'`
    if [ $value ]; then
      echo $value
    fi
  }
}

OP=$1

parse_out OVERALL RunTime
parse_out OVERALL Throughput
parse_out $OP AverageLatency
parse_out $OP 95thPercentileLatency
parse_out $OP 99thPercentileLatency
