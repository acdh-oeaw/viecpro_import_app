import logging

def init_logger(level, logfile):
    logger = logging.getLogger("import_logger")
    stream_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(logfile, mode="a")
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.setLevel(level)

def init_funclogger(level, logfile):
    funclogger = logging.getLogger("func_logger")
    file_handler = logging.FileHandler(logfile, mode="a")
    formatter = logging.Formatter("%(funcName)s >>> %(message)s")
    file_handler.setFormatter(formatter)
    funclogger.addHandler(file_handler)
    funclogger.setLevel(level)

def init_complogger(level, logfile):
    complogger = logging.getLogger("comp_logger")
    file_handler = logging.FileHandler(logfile, mode="a")
    formatter = logging.Formatter("NLP COMPONENT >>> %(filename)s >>> %(message)s")
    file_handler.setFormatter(formatter)
    complogger.addHandler(file_handler)
    complogger.setLevel(level)

def init_caselogger(level, logfile, case):
    caselogger = logging.getLogger("case_logger")
    file_handler = logging.FileHandler(logfile, mode="a")
    formatter = logging.Formatter(f"%(message)s")
    file_handler.setFormatter(formatter)
    caselogger.addHandler(file_handler)
    caselogger.setLevel(level)
