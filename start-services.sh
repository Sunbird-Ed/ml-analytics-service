#!/bin/bash

#start survey_realtime_streaming
/opt/sparkjobs/faust_as_service/faust.sh  survey/survey_realtime_streaming survey/ &

#start observation_realtime_streaming
/opt/sparkjobs/faust_as_service/faust.sh  observations/observation_realtime_streaming observations/ &

#start observations
#/opt/sparkjobs/faust_as_service/faust.sh  observations/py_observation_streaming observations/ &

#start observation_evidence
#/opt/sparkjobs/faust_as_service/faust.sh  observations/py_observation_evidence_streaming observations/ &

#start survey
#/opt/sparkjobs/faust_as_service/faust.sh  survey/py_survey_streaming survey/ &

#start survey_evidence
#/opt/sparkjobs/faust_as_service/faust.sh  survey/py_survey_evidence_streaming survey/ &

wait -n

exit $?
