"""
Detecting data aggregation - Multiplication

"""
import pandas as pd
import numpy as np
from functions import *

csvData = pd.read_csv("IndustrialCommwastebusiness.csv")
smallData = csvData.iloc[:,:]
# print(smallData)
pd.options.display.max_columns = None
# print(csvData.head())

dataDF = []
for rownr, rows in csvData.iterrows():
    newData = list(rows)
    dataDF.append(newData)

# print(dataDF)

# filter function - returns the numeric data from the dataset
filteredList = []
filterInt(dataDF, filteredList)
print("\n",filteredList)

# Split the data into many small datasets
def splitData(filteredList, newWorkingList):
    countLength = len(filteredList)
    for rownr, row in enumerate(filteredList):
        colCount = len(row)
        rowCount = countLength
        newWorkingList.append(filteredList[:30])
        break

# splitting data through splitData function in parallel processing
newWorkingList = []
splitData(filteredList, newWorkingList)
for rownr, row in enumerate(newWorkingList):
    print("\n",row)


# lookup dictionary for validation - numeric values
lookupDict = {}
for rownr, row in enumerate(filteredList):
    for cellnr, cell in enumerate(row):
        if cell != None:
            if cell not in lookupDict:
                lookupDict[cell] = set()
                lookupDict[cell].add((rownr, cellnr))
# print("LookUPDict:", lookupDict)


# lookup dictionary for column header extraction
lookupColumns = {}
for rownr, rows in csvData.iterrows():
    for colnr, col in rows.iteritems():
        if col == 'nan':
            continue
        if col not in lookupColumns:
            lookupColumns[col] = set()
            lookupColumns[col].add(rownr)

# columns in data list
def aggregationFunc():
    subsetSumDict = {}
    sumsDict = {}

    for rownr, row in enumerate(filteredList):
        notNoneCells = [cellnr for cellnr, cell in enumerate(row) if cell != None]
        print("\n",notNoneCells)
        for colSubsets in subsets(notNoneCells):
            if len(colSubsets) < 2:
                continue
            else:
                subsetSum = product(row[cellnr] for cellnr in colSubsets)
                if subsetSum in lookupDict:
                    colSet = lookupDict[subsetSum]

                    for rownr1, colnr1 in colSet:
                        rowMatch = (rownr, rownr1)
                        columnsMatch = (colSubsets, colnr1)

                        if columnsMatch not in subsetSumDict:
                            subsetSumDict[columnsMatch] = set()
                        subsetSumDict[columnsMatch].add(rowMatch)

                        if subsetSum not in sumsDict:
                            sumsDict[subsetSum]=set()
                        sumsDict[subsetSum].add(columnsMatch)

    print("\n", subsetSumDict)
    print("\n", sumsDict)
