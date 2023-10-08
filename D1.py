from mrjob.job import MRJob
#repartition join as datasets too large for memory of one machine
class repartition_D(MRJob):


    def mapper(self, _, line):

        try:
            if(len(line.split(','))==7):#aggregate transactions:
                fields=line.split(',')
                join_key=fields[2]#to adress
                join_value=int(float(fields[3]))#value
                yield (join_key,(join_value,1))

            else:#contracts
                fields=line.split(',')
                join_key=fields[0]#key is address
                join_value=fields[1]#scam id
                yield (join_key,(join_value,2))
        except:
            pass

    def reducer(self, address, values):
        Transactions = False
        scams = False
        total = 0
        scamID = []

        for value in values:
            if value[1]==1:
                Transactions=True
                total+=value[0]
            elif value[1]==2:
               scamID.append(value[0])
               scams=True
               #total += value[0]
        if Transactions and scams:
            yield (address, (scamID, total))

if __name__=='__main__':
    repartition_D.JOBCONF= { 'mapreduce.job.reduces': '15' }
    repartition_D.run()
#
