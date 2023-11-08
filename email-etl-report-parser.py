import pandas as pd
from bs4 import BeautifulSoup
import numpy as np 


def check_percent(df):
	# Takes a dataframe. returns false if there is a value not ends with '%'
	for i in df:
		if str(i) == '' or str(i) == '-':
			continue
		if str(i).endswith('%') == False:
			return False
		try:
			float(str(i).strip('%'))
			return True
		except ValueError:
			return False
		return True

def check_dolSign(df):
	# Takes a dataframe. returns false if there is a value not starts with '$'
	for i in df:
		
		if str(i) == '' or str(i) == '-':
			continue
		if str(i).startswith('$') == False:
			return False
		try:
			float(str(i).replace('$','').replace(',',''))
			return True
		except ValueError:
			return False

def dollar_strip(df):
	L = []
	for i in df:
		if i != '':
			try:
				i = str(i).replace('$','').replace(',','')
				L.append(i)
			except ValueError:
				L.append(i)
		else:
			L.append(i)
	L = pd.DataFrame(L)
	return L

def per_to_float(df):
	# Takes a dataframe, converts percentage to decimal, and returns the decimal df
	L = []
	for i in df:
		if i != '':
			try:
				i = float(str(i).strip('%'))/100.0
				L.append(i)
			except ValueError:
				L.append(i)
		else:
			L.append(i)
	L = pd.DataFrame(L)
	return L

def strip_arrows(df):
	for col in df.columns:
		if '↑' in col or '↓' in col:
			renamedcol = col.replace('↑', '').replace('↓', '')
			df.rename(columns={col: renamedcol}, inplace=True)

def normalize_symbols(df):
	columns_to_convert = []
	for col in df.columns[df.dtypes=='object']:
		if '✕' in df[col].values or '✔' in df[col].values:
			columns_to_convert.append(col)
	for col in columns_to_convert:
		for i, row in df.iterrows():
			cell = row[col]
			if cell == '✕':
				df.at[i, col] = 'false'
			elif cell == '✔':
				df.at[i, col] = 'true'
	return df

def main(resp):

	dataframe = None
	longest = 0
	# The largest html table is most likely the dataframe we are looking for.
	for df in blob_contents:
		if (len(df.index) > longest):
			dataframe = df
			longest = len(df.index)
	dataframe = dataframe.replace("-", None)
	# Loop through each column, check whether all the valeus in that column are percentage,
	# if yes, convert percentage to decimal
	for i in dataframe:
		df = dataframe[i]
		if check_percent(df) == True:
			dataframe[i] = per_to_float(df)
		if check_dolSign(df) == True:
			dataframe[i] = dollar_strip(df)
	strip_arrows(dataframe)
	dataframe = normalize_symbols(dataframe) # replace All the "X" and "Y" to bool values, False/True
	return dataframe
	
