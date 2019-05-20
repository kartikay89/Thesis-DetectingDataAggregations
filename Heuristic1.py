#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Detecting data aggregation - Sums (Single table)

"""

import pandas as pd
import numpy as np
from functions import *

csvData = pd.read_csv("IndustrialCommwastebusiness.csv")
pd.options.display.max_columns = None
print(csvData.head())

dataDF = []
for rownr, rows in csvData.iterrows():
    newData = list(rows)
    dataDF.append(newData)

# print(dataDF)

# filter function - returns the numeric data from the dataset
filteredList = []
filterInt(dataDF, filteredList)
print("\n",filteredList)

# lookup dictionary for validation - numeric values
lookupDict = {}
for rownr, row in enumerate(filteredList):
    for cellnr, cell in enumerate(row):
        if cell != None:
            if cell not in lookupDict:
                lookupDict[cell] = set()
                lookupDict[cell].add((rownr, cellnr))
print("LookUPDict:", lookupDict)
#
#
# # lookup dictionary for column header extraction
lookupColumns = {}
for rownr, rows in csvData.iterrows():
    for colnr, col in rows.iteritems():
        if col == 'nan':
            continue
        if col not in lookupColumns:
            lookupColumns[col] = set()
            lookupColumns[col].add(rownr)

print("\n", lookupColumns)
#
# validationKeys -> Consists of all columns headers(strings) and values(integers)
validationKeys = []
ColValidationDict = {} # The final cross validation dictionary for headers

columnKeys = lookupColumns.keys()
for index, values in lookupDict.items():
    print("\n", index, values)
    for keys in columnKeys:
        try:
            filterlookupColumns = int(keys.replace(",",""))
            validationKeys.append(filterlookupColumns)
            if filterlookupColumns == index:
                print(filterlookupColumns)
                filterSet = lookupDict[filterlookupColumns]
                print(filterSet)
            try:
                for value1 in filterSet:
                    if filterlookupColumns == index:
                        if filterlookupColumns not in ColValidationDict:
                            ColValidationDict[filterlookupColumns] = set()
                        ColValidationDict[filterlookupColumns].add(value1)
            except:
                pass
        except:
            validationKeys.append(keys)
            ColValidationDict[keys]=set()

# print("\n",validationKeys)
# print("\n",lookupColumns)
print("\n",ColValidationDict)
#
#columns in data list
subsetSumDict = {}
sumsDict = {}
for rownr, row in enumerate(filteredList):
    notNoneCells = [cellnr for cellnr, cell in enumerate(row) if cell != None]
    # print("\n",notNoneCells)
    for colSubsets in subsets(notNoneCells):
        if len(colSubsets) < 2:
            continue
        else:
            subsetSum = sum(row[cellnr] for cellnr in colSubsets)
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
print("\n", sumsDict) # sum: columns


# Cross Validation
DictColorCode = {} # contains all sums from subsets from the table with row, col
for value in sumsDict.keys():
    for key1, value1 in ColValidationDict.items():
        if value == key1:
            if value not in DictColorCode:
                DictColorCode[value]=set()
            DictColorCode[value].add(value)

            # print("Value cross present: ", value, value1)
#
#
# Columns occuring most frequently:
sortedMatches = sorted(subsetSumDict.items(), key=lambda kv: -len(kv[1]))
for (subsetSum, colnr), row_matches in sortedMatches[:100]:
    print('Columns', subsetSum, 'match to column', colnr, 'for',row_matches, 'rows')
# Write a file with the results:
with open("output.txt", 'w') as write_file:
    for (subsetSum, colnr), row_matches in sortedMatches:
        print(('Columns', subsetSum, 'match to column', colnr, 'for', len(row_matches), 'rows'), file=write_file)

# Color coded DataFrame - DictColorCode
# Inline DataFrame
input_DFDict = {}

csvDataToDict = csvData.to_dict()
# print("\n",csvDataToDict)
DFConverter(csvDataToDict, input_DFDict)

# outputList contains the DF with integers
outputList = []
# print("\n", outputList)
DFIntConverter(dataDF, outputList)
# print("\n", outputList)

# converting the outputList to dictionary
items = {key : value for (key, value) in enumerate(outputList)}
# print(items)
# newDframe is the changed inline dataframe
newDframe = pd.DataFrame.from_dict(items, orient='index')
# print(newDframe)
newDframe.to_csv("result.csv")
#
# # TODO: Create a scoring technique
#
# # TODO: fetch column names
#
# # TODO: color code the results


# # s(newDframe.to_csv("result1.csv"))
