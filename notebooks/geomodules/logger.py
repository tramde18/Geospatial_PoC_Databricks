import logging 


class Logger():
    logging.basicConfig(level=logging.INFO)
    
    @staticmethod
    def displayInfo(msg):
        logging.info(msg)

    @staticmethod
    def displayWarning(msg):
        logging.warn(msg)

    @staticmethod
    def displayError(msg):
        logging.error(msg)