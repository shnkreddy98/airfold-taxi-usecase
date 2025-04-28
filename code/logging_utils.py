import logging
import sys
import os
from datetime import datetime

LOGSDIR = './logs'

# Configure logging
def setup_logging(log_file=None):
    """Set up logging to both console and file"""
    if log_file is None:
        # Create logs directory if it doesn't exist
        if not os.path.exists(LOGSDIR):
            os.makedirs(LOGSDIR)
        # Create log file with timestamp in name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(LOGSDIR, f"Ingest_{timestamp}.log")

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers = []
    
    # Format for log messages
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', 
                                  datefmt='%Y-%m-%d %H:%M:%S')
    
    # File handler for logging to file
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler for logging to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    logging.info(f"Logging initialized - writing to {log_file}")
    return log_file

# Function to help migrate from print statements to logging
def log_print(*args, level=logging.INFO, **kwargs):
    """Replacement for print that logs to both console and file"""
    message = ' '.join(str(arg) for arg in args)
    logging.log(level, message)