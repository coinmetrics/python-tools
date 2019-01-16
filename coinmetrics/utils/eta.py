from datetime import datetime, timedelta
from logging import Logger
from collections import deque


class _ObservationHistory(object):

    def __init__(self, maxObservations: int):
        self._maxObservations = maxObservations
        self._observations = deque()

    def addObservation(self, timeSpent: float, workDone: int):
        self._observations.append((timeSpent, workDone))
        if len(self._observations) > self._maxObservations:
            self._observations.popleft()

    def getAverage(self) -> float:
        totalWork = 0
        totalTime = 0
        for timeSpent, workDone in self._observations:
            totalTime += timeSpent
            totalWork += workDone
        return totalTime / totalWork


class ETA(object):

    def __init__(self,
                 log: Logger,
                 totalWorkAmount: int,
                 maxObservations: int,
                 outputInterval: int,
                 printPrefix: str=""):
        assert totalWorkAmount > 0, "total work amount should be positive"
        self._log = log
        self._totalWorkAmount = totalWorkAmount
        self._printPrefix = printPrefix
        if len(self._printPrefix) > 0:
            self._printPrefix += " "
        self._workDone = 0
        self._history = _ObservationHistory(maxObservations)
        self._workStartedFlag = False
        self._workStartedTime = datetime(1970, 1, 1)
        self._outputInterval = outputInterval
        self._workSteps = 0

    def workStarted(self):
        assert not self._workStartedFlag, "work shouldn't have been already started"
        self._workStartedFlag = True
        self._workStartedTime = datetime.now()

    def workFinished(self, workDone: int):
        assert self._workStartedFlag, "work should've been started"
        self._workStartedFlag = False
        self._updateWork(workDone, datetime.now() - self._workStartedTime)

    def _updateWork(self, workDone: int, timeSpent: timedelta) -> bool:
        self._history.addObservation(timeSpent.total_seconds(), workDone)
        self._workDone += workDone
        self._workSteps += 1

        if self._workSteps % self._outputInterval == 0:
            self.output()
            return True
        else:
            return False

    def getETA(self) -> float:
        return (self._totalWorkAmount - self._workDone) * self._history.getAverage()

    def getPercentDone(self) -> float:
        return float(self._workDone) / self._totalWorkAmount

    def output(self):
        self._log.info("{0}{1:0.2f}% done, eta is {2}, speed is {3:0.4f}/s".format(
            self._printPrefix,
            100.0 * self.getPercentDone(),
            self.prettyStringForETA(self.getETA()),
            1.0 / self._history.getAverage()))

    def prettyStringForETA(self, etaInSeconds: float) -> str:
        totalSeconds = int(etaInSeconds)

        daysModulo = totalSeconds % (3600 * 24)
        days = (totalSeconds - daysModulo) // (3600 * 24)
        totalSeconds -= days * (3600 * 24)

        hoursModulo = totalSeconds % 3600
        hours = (totalSeconds - hoursModulo) // 3600
        totalSeconds -= hours * 3600

        minutesModulo = totalSeconds % 60
        minutes = (totalSeconds - minutesModulo) // 60
        seconds = totalSeconds - minutes * 60

        return "{0} days {1} hours {2} minutes {3} seconds".format(days, hours, minutes, seconds)
