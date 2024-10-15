import logging
import os
from datetime import datetime

def setup_logger(name):
    # Create a custom logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    os.makedirs("logs", exist_ok=True)

    # Create a file handler
    current_datetime = datetime.now().strftime("%Y-%m-%d")
    # Create a file handler with datetime-appended log file name
    log_file = os.path.join("logs" ,f"log_{current_datetime}.log")
    f_handler = logging.FileHandler(log_file)
    f_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handler
    f_format = logging.Formatter('%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)

    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handler
    c_handler.setFormatter(f_format)

    # Add handler to the logger
    if not logger.handlers:  # To prevent adding multiple handlers to the same logger
        logger.addHandler(f_handler)
        logger.addHandler(c_handler)

    return logger
