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
