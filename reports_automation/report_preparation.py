import sys
sys.path.insert(0, '/opt/sparkjobs/ml-analytics-service/reports_automation/')

from get_token import get_access_token
from report_create import backEnd_create,frontEnd_create

access_token = None
response_api = get_access_token()
if response_api["status_code"] == 200:
   access_token = response_api["result"]["access_token"]


backEnd_create("ml_test_automation.json")

if (access_token!= None):
    frontEnd_create(access_token,"ml_test_automation.json")
