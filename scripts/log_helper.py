import logging

# Setup logging to both console and file.
def init_logging(file_name):
    root_logger = logging.getLogger()
    log_formatter = logging.Formatter("%(asctime)s [%(levelname)s]  %(message)s")

    file_handler = logging.FileHandler(file_name)
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)
    
    root_logger.setLevel(logging.DEBUG)
