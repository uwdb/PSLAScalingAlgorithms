import random

listRands = []

data1 = []
data2 = []

with open('queries_underestimated_estimated.csv','r') as source:
	for line in source:
		currentRand = random.random()
		data1.append((currentRand, line))
		listRands.append(currentRand)

counter = 0
with open('queries_underestimated_actual.csv','r') as source:
	for line in source:
		data2.append((listRands[counter], line))
		counter = counter + 1

data1.sort()
data2.sort()

with open('randomOrder/queries_underestimated_estimated_rand1.csv','w') as target:
   for _, line in data1:
       target.write( line )

with open('randomOrder/queries_underestimated_actual_rand1.csv','w') as target:
   for _, line in data2:
       target.write( line )