
from mrjob.job import MRJob
import re# this is a regular expression that finds all the words inside a String
import time
import datetime
# this line declares the class Lab1, that extends the MRJob format.
class courseworkB1(MRJob):

    def mapper(self, _, line):
        #do split
        #correct charector for splitting
        try:
            fields = line.split(',')
            if len(fields) == 7:

                to_address = (fields[2])
                value = int(fields[3])
                yield(to_address, value)
        except:
            pass


    def reducer(self, key, counts):
        total = sum(counts)#int(fields[1])
        yield (key, total)


    def combiner(self, key, counts):
        total = sum(counts)#int(fields[1])
        yield(key, total)


if __name__ == '__main__':
    courseworkB1.run()
