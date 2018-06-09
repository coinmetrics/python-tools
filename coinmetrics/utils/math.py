def meanOf(series):
	return sum(series) / len(series)

def stddevOf(series):
	seriesMean = meanOf(series)
	sumSqr = 0.0
	for elem in series:
		deviation = seriesMean - elem
		sumSqr += deviation * deviation
	sumSqr /= len(series) - 1
	return sumSqr**0.5