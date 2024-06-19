from utils.utils import loadFromSpark

class DescriptiveAnalysis():

    def __init__(self, spark, mongoLoader, logger):
        self.spark = spark
        self.mongoLoader = mongoLoader
        self.logger = logger

        self.collections = ['airqual', 'idealista', 'income']


    def main(self):
        dfs = loadFromSpark(self.spark, self.mongoLoader,
                            self.logger, self.collections)
