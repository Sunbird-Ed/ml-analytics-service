#API status code
success_code = 200
success_code1 = 201

#API ENDPOINTS
refresh_token = "auth/realms/sunbird/protocol/openid-connect/token"
access_token = "auth/v1/refresh/token"
backend_create = "api/data/v1/report/jobs/submit"
frontend_create = "/api/data/v1/report-service/report/create"
frontend_get = "/api/data/v1/report-service/report/get/"
backend_update = "/api/data/v1/report/jobs/"
frontend_update = "/api/data/v1/report-service/report/update/"
frontend_retire = "/api/data/v1/report-service/report/delete/"
backend_retire = "/api/data/v1/report/jobs/deactivate/"
reports_list = "/api/data/v1/report-service/report/list"

#API Headers
content_type = "application/json"
content_type_url = "application/x-www-form-urlencoded"

# Mongo collections 
reports_log_collec = "reports_logs"
