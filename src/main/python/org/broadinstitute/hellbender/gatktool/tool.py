"""
Functions and classes used to extend a GATK tool with Python.

GATK uses two FIFOs to communicate wth Python. The "ack" FIFO is read by GATK
and written by Python code, and is used to signal that a Python command has
completed execution. The "data" FIFO is written by GATK and read by Python,
and is used to pass data to Python from Java.

Most of the functions in this module are intended to be called by GATK via
the StreamingPythonScriptExecutor Java class, and are not called by Python
code directly. The one exception is the readDataFIFO function, which can be
used to read data that had been passed to Python by GATK Java code.
"""

import sys
import os

_ackFIFO = None
_dataFIFO = None

def onTraversalStart(ackFIFOName: str):

    """
    Open the GATK ack FIFO and install the exception handler hook.

    Called by GATK when the StreamingPythonScriptExecutor is initialized,
    which is normally in onTraversalStart.
    """
    # the exception hook uses the ack FIFO, so initialize the FIFO before
    # installing it
    global _ackFIFO
    _ackFIFO = AckFIFO(ackFIFOName)
    sys.excepthook = gatkExceptionHook


def gatkExceptionHook(exceptionType, value, traceback):

    """
    GATK Handler for uncaught Python exceptions.

    The is installed by onTraversalStart after the ack fifo has been
    initialized. The handler sends a nack to GATK, which results in a
    PythonScriptExecutor exception being thrown.
    """
    sendNack()
    sys.__excepthook__(exceptionType, value, traceback)


def sendAck():
    global _ackFIFO
    _ackFIFO.writeAck()


def sendNack():
    global _ackFIFO
    _ackFIFO.writeNack()


def onTraversalSuccess():
    # TODO: this really should reset the global exception hook...
    pass


def closeTool():
    global _ackFIFO
    assert _ackFIFO != None
    _ackFIFO.close()
    _ackFIFO = None


def initializeDataFIFO(dataFIFOName: str):
    global _dataFIFO
    _dataFIFO = DataFIFO(dataFIFOName)


def closeDataFIFO():
    global _dataFIFO
    assert _dataFIFO != None
    _dataFIFO.close()
    _dataFIFO = None


def readDataFIFO() -> str:
    global _dataFIFO
    return _dataFIFO.readLine()


class AckFIFO:
    """
    Manage the FIFO used to notify GATK (via an ack) that a command has
    completed, or failed due to an unhandled exception (via a nck).
    """
    _ackString = "ack"
    _nackString = "nck"

    def __init__(self, ackFIFOName: str):
        """Open the ack fifo stream for writing only"""
        self.ackFIFOName = ackFIFOName
        writeDescriptor = os.open(self.ackFIFOName, os.O_WRONLY)
        self.fileWriter = os.fdopen(writeDescriptor, 'w')

    def writeAck(self):
        assert self.fileWriter != None
        self.fileWriter.write(AckFIFO._ackString)
        self.fileWriter.flush()

    def writeNack(self):
        assert self.fileWriter != None
        self.fileWriter.write(AckFIFO._nackString)
        self.fileWriter.flush()

    def close(self):
        assert self.fileWriter != None
        self.fileWriter.close()
        self.fileWriter = None


class DataFIFO:
    """
    Manage the FIFO stream used for transferring data from the GATK tool to
    Python code.

    The FIFO is written by GATK and read by Python.
    """
    # TODO: Need a way to provide a per-line deserializer
    def __init__(self, dataFIFOName: str):
        """Open the data stream fifo for reading"""
        self.dataFIFOName = dataFIFOName

        # the data fifo is always opened for read only on the python side
        readDescriptor = os.open(self.dataFIFOName, os.O_RDONLY)
        self.fileReader = os.fdopen(readDescriptor, 'r')

    def readLine(self) -> str:
        assert self.fileReader != None
        return self.fileReader.readline()

    def close(self):
        assert self.fileReader != None
        self.fileReader.close()
        self.fileReader = None
