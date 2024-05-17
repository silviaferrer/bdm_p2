import os

from utils.loadtoMongo import MongoLoader
from utils.formatedLoader import LoadtoFormatted

def main():
    
    #######################Load data to MongoDB#####################
    mongoLoader = MongoLoader()
    ################################################################

    formatedLoader = LoadtoFormatted()

    #######################
    return None 

if __name__ == '__main__':
    main()