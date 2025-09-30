import json
from configparser import ConfigParser
from services.database_service import DBConnection

class Environment:
    def __init__(self):
        self.configDS = './config/datastore.ini'
        self.config_ini = ConfigParser()
        self.config_ini.read(self.configDS)       
        self.config = {}

        for section in self.config_ini.sections():
            self.config[section] = {}
            for key, value in self.config_ini.items(section):
                self.config[section][key] = value

    def getConfig(self):
        return self.config

# Global instance
env = Environment()
