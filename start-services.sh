#!/bin/bash

#start observations
/opt/sparkjobs/faust_as_service/faust.sh  observations/py_observation_streaming observations/ &

#start observation_evidence
/opt/sparkjobs/faust_as_service/faust.sh  observations/py_observation_evidence_streaming observations/ &

#start survey
/opt/sparkjobs/faust_as_service/faust.sh  survey/py_survey_streaming survey/ &

#start survey_evidence
/opt/sparkjobs/faust_as_service/faust.sh  survey/py_survey_evidence_streaming survey/ &

#start run.sh
/opt/sparkjobs/ml-analytics-service/run.sh > /opt/sparkjobs/ml-analytics-service/run-sh.log &

wait -n

exit $?
