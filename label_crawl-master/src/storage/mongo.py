from nb_conn.mongodb.async_helper import MongodbHelper


class MongodbStorage:

    def __init__(self, mongo_config):
        self.tag_mongo_config = mongo_config["tag"]
        self.data_mongo_config = mongo_config["data"]
        self.tag_algorithm_config = mongo_config["tag_algorithm"]
        self.tag_mongo = MongodbHelper(self.tag_mongo_config["client"])
        self.data_mongo = MongodbHelper(self.data_mongo_config["client"])
        self.tag_algorithm_mongo = MongodbHelper(self.tag_algorithm_config["client"])


