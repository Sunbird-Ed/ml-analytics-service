import os , datetime , json
from configparser import ConfigParser,ExtendedInterpolation
from pymongo import MongoClient

# Reading the config file
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(f"{config_path[0]}/config.ini")


class Database:

    def __init__(self):
        '''
        Initializes Database connection
        '''
        clientProd = MongoClient(config.get('MONGO', 'url'))
        self.db = clientProd[config.get('MONGO', 'database_name')]
        today = datetime.date.today()
        self.today = today.strftime("%d-%B-%Y")

    def connection(self):
        '''
            Return Connection Instance
        '''
        return self.db