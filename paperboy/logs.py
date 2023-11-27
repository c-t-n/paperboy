import logging


class PaperboyFormatter(logging.Formatter):
    grey = "\x1b[38;21m"
    blue = "\x1b[34;21m"
    yellow = "\x1b[33;21m"
    red = "\x1b[31;21m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format_str = "%(levelname)s | %(name)s | %(message)s"  # type: ignore

    FORMATS = {
        logging.DEBUG: f"{blue}{format_str}{reset}",
        logging.INFO: f"{format_str}",
        logging.WARNING: f"{yellow}{format_str}{reset}",
        logging.ERROR: f"{red}{format_str}{reset}",
        logging.CRITICAL: f"{bold_red}{format_str}{reset}",
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
