#!/bin/bash

#start survey_realtime_streaming
/opt/sparkjobs/faust_as_service/faust.sh  survey/survey_realtime_streaming survey/ &

wait -n

exit $?
