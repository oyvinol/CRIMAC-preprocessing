import os
from sys import platform
import time


def fileInDirectory(directory: str):
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    return files

def listComparison(oldFileList: list, newFileList: list):
    newFilesAdded = [x for x in newFileList if x not in oldFileList] #Note if files get deleted, this will not highlight them
    return newFilesAdded

def fileWatcher(inputDirectory: str, outStr, callbackFunction = None):
    """
    Monitor directory and trigger callback on file(s) added. 
    Callback is triggered with list of new files. 
    Code 'inspired' by https://towardsdatascience.com/implementing-a-file-watcher-in-python-73f8356a425d

    outStr is supplied to callback with file list for configuring output.
    Callback should look like this:
      def callback(fileDiff: list, ouputStr: str)
    """

    while True:
        if 'watching' not in locals():
            previousFileList = fileInDirectory(inputDirectory)
            watching = 1 #triggers above
            print(f"First check of input directory {inputDirectory} starting..")
            print(previousFileList)

        time.sleep(1)

        try:
            newFileList = fileInDirectory(inputDirectory)
            fileDiff = listComparison(previousFileList, newFileList)

            previousFileList = newFileList
            if len(fileDiff) == 0:
                continue

            # Get absolute path for input files 
            absPathFiles = []
            for file in fileDiff:
                absPathFiles.append(inputDirectory + os.sep + file)

            if callbackFunction is not None:
                try:
                    callbackFunction(absPathFiles, outStr) 
                except Exception as e:
                    print(f"Callback function failed because {e}")

        except Exception as e:
            print(f"Failed to update file list because {e}")
            continue