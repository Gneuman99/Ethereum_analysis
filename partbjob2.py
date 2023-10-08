from mrjob.job import MRJob
#repartition join as datasets too large for memory of one machine
class repartition_stock_join(MRJob):

    def mapper(self, _, line):
        try:
            if(len(line.split('\t'))==2):#aggregate:
                fields=line.split('\t')
                join_key=fields[0]#to adress
                join_value=fields[1]#value
                yield (join_key,(join_value,1))

            elif(len(line.split(','))==5):#contracts
                fields=line.split(',')
                join_key=fields[0]#key is add
                join_value=(fields[0])#add
                yield (join_key,(join_value,2))
        except:
            pass
    def reducer(self, add, values):#if adress from job1 not within contracts, filter outas user address
        Transactions = False
	    contracts = False
	    total = 0

        for value in values:
            if value[1]==1:#transaction
            	Transaction = True
		total+=value[0]
                
            if value[1]==2:
            	contracts = True
        if Transactions and contracts == True:
            yield (add, total)
if __name__=='__main__':
    repartition_stock_join.run()
