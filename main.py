import os

from utils.loadtoMongo import MongoLoader
from utils.formattedLoader import LoadtoFormatted
from utils.dataFormatter import DataFormatter


def main():

    ####################### Load data to MongoDB#####################
    #mongoLoader = MongoLoader()
    #################################################################

    formattedLoader = LoadtoFormatted()

    dataFormatter = DataFormatter(formattedLoader.dfs)

    #formattedLoader.spark.stop()
    #######################
    return None


if __name__ == '__main__':
    main()
