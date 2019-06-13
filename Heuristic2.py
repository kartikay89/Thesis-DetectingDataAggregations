"""
Detecting data aggregation - Multiplication

"""
import pandas as pd
import numpy as np
from collections import Counter
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
print(len(filteredList))

# Split the data into many small datasets
# def splitData(filteredList, newWorkingList):
#     countLength = len(filteredList)
#     for rownr, row in enumerate(filteredList):
#         colCount = len(row)
#         rowCount = countLength
#         newWorkingList.append(filteredList[:30])
#         break

# splitting data through splitData function in parallel processing
# newWorkingList = []
# splitData(filteredList, newWorkingList)
# for rownr, row in enumerate(newWorkingList):
#     print("\n",row)


# lookup dictionary for validation - numeric values
lookupDict = {}
for rownr, row in enumerate(filteredList):
    for cellnr, cell in enumerate(row):
        if cell != None:
            if cell not in lookupDict:
                lookupDict[cell] = set()
            lookupDict[cell].add((rownr, cellnr))
print("LookUPDict:", lookupDict)

# Scoring lookup dict key:rownr
ScorelookupDict = {}
for rownr, row in enumerate(filteredList):
    for cellnr, cell in enumerate(row):
        if cell != None:
            if cell not in ScorelookupDict:
                ScorelookupDict[cell] = set()
            ScorelookupDict[cell].add(rownr)
print("ScorelookupDict:", ScorelookupDict)


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

subsetSumDict = {}
sumsDict = {}

for rownr, row in enumerate(filteredList):
    notNoneCells = [cellnr for cellnr, cell in enumerate(row) if cell != None]
    # print("\n",notNoneCells)
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
                    sumsDict[subsetSum].add(rowMatch)

# print("\n", subsetSumDict)
# print("\n", sumsDict)

# Scoring function - sumsDict and lookupDict
matches = []
def matching(dict1, dict2):
    for key1, value1 in dict1.items():
        for key2, value2 in dict2.items():
            if key1==key2:
                # print(key1)
                matches.append(key1)

print(matching(ScorelookupDict, sumsDict))
print(matches)

# Comparing matches to lookupDict
def score(match, row_index_list):
    return len(row_index_list)/len(filteredList)

# finalDict = {}

# for key, values in ScorelookupDict.items():
#     for rownr, row in enumerate(matches):
#         if row==key:
            # print(row,values)
best_match_per_column = {}
for match, row_index_list in subsetSumDict.items():
    try:
        scores = score(match, subsetSumDict[match])
        col_index_subset, agg_col_index = match
        _, _, best_score = best_match_per_column.get(agg_col_index, (None, None, 0))
        if scores > best_score:
            best_match_per_column[agg_col_index] = (col_index_subset, row_index_list, scores)
    except:
        pass

print(best_match_per_column)

# Write file:
with open('output_fileMul.txt', 'w') as fw:
    for agg_col_index in sorted(best_match_per_column, key=lambda agg_col_index: best_match_per_column[agg_col_index][2]) [::-1] :
        col_index_subset, row_index_list, score = best_match_per_column[agg_col_index]
#         print('Indices %s sum to index %d with score %.4f!' % (col_index_subset, agg_col_index, score))
        row_index_pairs = set((i,i) for i in row_index_list)
        print('Columns[%s,%d]/Rows[%s]\t%.5f' % (tuple(col_index_subset),agg_col_index, row_index_pairs, score ), file=fw)
