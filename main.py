import os

from utils.loadtoMongo import MongoLoader
from utils.formatedLoader import LoadtoFormatted
from utils.dataFormatter import DataFormatter


def main():

    ####################### Load data to MongoDB#####################
    mongoLoader = MongoLoader()
    ################################################################

    formatedLoader = LoadtoFormatted()

    dataFormatter = DataFormatter(formatedLoader.dfs)

    formatedLoader.spark.stop()
    #######################
    return None


if __name__ == '__main__':
    main()
