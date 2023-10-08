from mrjob.job import MRJob
class courseworkD11(MRJob):

    def mapper(self, _, line):
        #do split
        #correct charector for splitting
        try:
            fields = line.split('\t')
            if len(fields) == 2:
                #address = fields[0]
                fields = fields[1].split(',')
                scamID=fields[0]
                value=fields[1]
                value=int(value[1:len(value)-1])
                #yield(address, 1,( scamID))
                yield(scamID,value)
        except:
            pass


    def reducer(self, key, counts):
        total = sum(counts)#int(fields[1])
        yield (key, total)


    def combiner(self, key, counts):
        total = sum(counts)#int(fields[1])
        yield(key, total)


if __name__ == '__main__':
    courseworkD11.run()
