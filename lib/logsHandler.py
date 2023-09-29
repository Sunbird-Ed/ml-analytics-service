import os , datetime , lib.constants as constants , re

class Logs:

    def __init__(self,logFilePath):
        '''
        Initializes Logs variables
        '''
        current_date = datetime.date.today()
        self.formatted_current_date = current_date.strftime("%d-%B-%Y")
        number_of_days_logs_kept = current_date - datetime.timedelta(days=int(constants.logFileExpiryDays))
        self.number_of_days_logs_kept = number_of_days_logs_kept.strftime("%d-%B-%Y")
        self.logFilePath = logFilePath
    
    def constructOutputLogFile(self):
        return f"{self.logFilePath}{self.formatted_current_date}-output.log"
 
    def constructDebugLogFile(self):
        return f"{self.logFilePath}{self.formatted_current_date}-debug.log"
    

    def flushLogs(self):
        
        # Remove old log entries
        files_with_date_pattern = [file 
        for file in os.listdir(self.logFilePath) 
        if re.match(r"\d{2}-\w+-\d{4}-*", file)]

        for file_name in files_with_date_pattern:
            file_path = os.path.join(self.logFilePath, file_name)
            if os.path.isfile(file_path):
                file_date = file_name.split('.')[0]
                date = file_date.split('-')[0] + '-' + file_date.split('-')[1] + '-' + file_date.split('-')[2]
                if date < self.number_of_days_logs_kept:
                    os.remove(file_path)

