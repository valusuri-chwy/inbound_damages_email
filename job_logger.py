import logging
from pathlib import Path

# Create a single log file path
LOG_FILE = Path(__file__).parent / "run_log.log"

# Configure logging to append to a single file
logging.basicConfig(
    filename=LOG_FILE,
    filemode='a',
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

def log_start():
    logging.info("Job started.")

def log_success(rows_inserted: int, total_damage: float):
    logging.info(f"Job succeeded | Rows inserted: {rows_inserted} | Total damage cost: ${total_damage:,.2f}")

def log_error(error: Exception):
    logging.error(f"Job failed | Error: {str(error)}", exc_info=True)
