import os

from configparser import ConfigParser
from config.singleton import SingletonDecorator
from attrdict import AttrDict

CONFIG_FILE_NAME = "source.ini"

CONFIG_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                CONFIG_FILE_NAME)


@SingletonDecorator
class GlobalConfig:
    def __init__(self):
        self.db_config = ConfigParser()
        if not os.path.exists(CONFIG_FILE_PATH):
            raise FileExistsError(CONFIG_FILE_PATH)
        self.db_config.read(CONFIG_FILE_PATH)

    @property
    def database_config(self):
        return AttrDict({
            "postgres":
            "dbname={} user={} password={} host={}".format(
                self.db_config["POSTGRES"]["dbname"],
                self.db_config["POSTGRES"]["user"],
                self.db_config["POSTGRES"]["password"],
                self.db_config["POSTGRES"]["host"]),
            "redis":
            "redis://{}:{}/{}".format(self.db_config["REDIS"]["host"],
                                      self.db_config["REDIS"]["port"],
                                      self.db_config["REDIS"]["db"])
        })

    @property
    def postgres(self):
        config_dict = {
            "postgres":
            "dbname={} user={} password={} host={}".format(
                self.db_config["POSTGRES"]["dbname"],
                self.db_config["POSTGRES"]["user"],
                self.db_config["POSTGRES"]["password"],
                self.db_config["POSTGRES"]["host"]),
            "dbname":
            self.db_config["POSTGRES"]["dbname"],
            "user":
            self.db_config["POSTGRES"]["user"],
            "password":
            self.db_config["POSTGRES"]["password"],
            "host":
            self.db_config["POSTGRES"]["host"],
        }
        return AttrDict(config_dict)
