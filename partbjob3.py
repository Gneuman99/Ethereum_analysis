from mrjob.job import MRJob

class top_movements(MRJob):
	def mapper(self,_,line):
		try:
			fields=line.split('\t')
			if len(fields)==2:
				add=fields[0]
				values=int(fields[1])
				yield(None, (add,values))
		except:
			pass
	def combiner(self,_,values):
		sorted_values=sorted(values,reverse=True,key=lambda tup:tup[1])
		i=0
		for value in sorted_values:
			yield("top",value)
			i+=1
			if i >=10:
				break
	def reducer(self,_,values):
		sorted_values=sorted(values,reverse=True,key=lambda tup:tup[1])
		sorted_ten = sorted_values[0:10]
		for value in sorted_ten:
			yield((value[0],value[1]),None)

if __name__ == '__main__':
	top_movements.run()
