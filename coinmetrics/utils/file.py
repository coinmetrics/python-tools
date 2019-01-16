import os
import shutil
from typing import List
from random import randint
from datetime import datetime
from coinmetrics.utils.timeutil import datetimeToTimestamp


class AtomicallySwappableFile(object):

    def __init__(self, fileName: str, path: str):
        self._fileName = fileName
        self._path = path
        if self._path[-1] != "/":
            self._path += "/"

        self._currentVersion = self._initSymlink()
        self._cullOldVersions()

    def update(self, content: str):
        newVersion = self._createNewVersionFile()
        newVersionPath = self._getVersionPath(newVersion)
        with open(newVersionPath, "w") as f:
            f.write(content)
            os.fsync(f)

        tmpName = self._path + self._getNewVersionId()
        os.symlink(self._getVersionFileName(newVersion), tmpName)
        shutil.move(tmpName, self._path + self._fileName)

        self._currentVersion = newVersion
        self._cullOldVersions()

    def read(self) -> str:
        if self._currentVersion is not None:
            with open(self._getVersionPath(self._currentVersion), "r") as f:
                return f.read()
        else:
            return ""

    def _initSymlink(self) -> str:
        symlinkPath = self._path + self._fileName
        if not os.path.islink(symlinkPath):
            newVersionId = self._createNewVersionFile()
            os.symlink(self._getVersionFileName(newVersionId), symlinkPath)
            return newVersionId
        else:
            versions = self._collectVersions()
            try:
                currentVersion = os.readlink(symlinkPath).split("/")[-1][len(self._fileName) + 1:]
                if currentVersion not in versions:
                    raise Exception("file {0} is corrupted: symlink points to non-existent version".format(symlinkPath))
                return currentVersion
            except Exception as e:
                return None

    def _collectVersions(self) -> List[str]:
        result = []
        files = [f for f in os.listdir(self._path) if os.path.isfile(os.path.join(self._path, f))]
        for f in files:
            pieces = f.split(".")
            if ".".join(pieces[0:-1]) == self._fileName:
                result.append(pieces[-1])
        return result

    def _createNewVersionFile(self) -> str:
        versionId = self._getNewVersionId()
        with open(self._getVersionPath(versionId), "w") as f:
            f.write("\n")
        return versionId

    def _getVersionPath(self, version: str) -> str:
        return self._path + self._fileName + "." + version

    def _getVersionFileName(self, version: str) -> str:
        return self._fileName + "." + version

    def _cullOldVersions(self):
        versions = self._collectVersions()
        for version in versions:
            if version != self._currentVersion:
                os.remove(self._getVersionPath(version))

    def _getNewVersionId(self) -> str:
        return str(int(datetimeToTimestamp(datetime.utcnow()) * 1000)) + "_" + str(randint(1, 2**64))

    def _getTimeFromVersionId(self, version: str) -> datetime:
        return datetime.utcfromtimestamp(version.split("_")[0])
