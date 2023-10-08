#Lab 2. Basic wordcount
from mrjob.job import MRJob
import re# this is a regular expression that finds all the words inside a String
from mrjob.job import MRJob
import re# this is a regular expression that finds all the words inside a String
import time
import datetime
# this line declares the class Lab1, that extends the MRJob format.
class partc(MRJob):
	def mapper(self, _, line):
        #do split
        #correct charector for splitting
        	try:

            		fields = line.split(',')
            		if len(fields) == 9:

                		miner = fields[2]
                		size = int(fields[4])
                		yield(miner, size)
        	except:

           	        pass

	def reducer(self, key, counts):
	#	sorted_values=sorted(values,reverse=True,key=lambda tup:tup[2])
	#	sorted_ten=sorted_values[0:10]
	#	for value in sorted_ten:
	#		yield((value[0],value[1]),None)
        	total = sum(counts)#int(fields[1])
        	yield (key, total)

	def combiner(self, key, counts):
        	total = sum(counts)#int(fields[1])
        	yield(key, total)


if __name__ == '__main__':
	partc.run()
