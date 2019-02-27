#!/bin/bash
for i in {1..50}
do
   go test -run | egrep -v "EOF|unix|Register|shut"
done > test_50.log
