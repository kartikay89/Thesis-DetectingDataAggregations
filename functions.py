import numpy as np
import pandas as pd

# Replace comma from numbers and return integers
def filter(value):
	try:
		return int(value.replace(",",""))
		print(value)
	except:
		try:
			floatVal = float(value)
			return int(round(floatVal))
		except:
			pass


# data filter
def filterInt(inputlists, outputlist):
	for rownr, rows in enumerate(inputlists):
		filterdata = [filter(cells) for cellnr, cells in enumerate(rows)]
		outputlist.append(filterdata)

		# print(rownr, rows)
		# print(filterdata)

# rounding values in DF
def rounded(value):
	try:
		floatVal = float(value)
		return int(round(floatVal))
	except:
		pass

def roundFloat(inputlists, outputlist):
	for rownr, rows in enumerate(inputlists):
		filterdata = [rounded(cells) for cellnr, cells in enumerate(rows)]
		outputlist.append(filterdata)

# subsets
def subsets(xs, i=0):
    if i == len(xs):
        yield ()
        return
    for c in subsets(xs,i+1):
        yield c
        yield c+(xs[i],)

# Inline DataFrame converter
def DFConverter(dictFromDataFrame, dict):
	for index, value in dictFromDataFrame.items():
		# print(index, value)
		for key, val in value.items():
			# print(key, val)
			if key not in dict:
				dict[key]=set()
			dict[key].add(val)

# Converting dict values to integer to merge in dataframe
def convertFilter(value):
	try:
		return int(value.replace(",",""))
	except:
		return value

# This function will only convert numbers to integer in dataframe and leave the rest
def DFIntConverter(values, outputList):
	for rownr, row in enumerate(values):
			DFInt = [convertFilter(cell) for cellnr, cell in enumerate(row)]
			# print(DFInt)
			outputList.append(DFInt)

# Color coding function
def colorCode(val):
	color='darkblue'
	return 'color: %s' % color

def colorCodeNA(val):
	color='red'
	return 'color: %s' % color

# Product function
def product(list):
	try:
		result=1
		for x in list:
			result = result*x
		return result
	except:
		return None
