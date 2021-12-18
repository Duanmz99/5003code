import random
import numpy as np
import math
from hashlib import sha1
import findspark
findspark.init('../hadoop/spark-3.0.3')
import time
import pyspark
import random
class hll_demo(object):
    def __init__(self, num_range, num_total,b=8,lista=[]):
        # set hash function, default hash table, b value
        self.hashfunc=sha1
        self.bucket={}
        self.b=b
        for i in range(pow(2, self.b)):
            key=bin(i)[2:].zfill(self.b)
            self.bucket[key]=0
        self.num_range=num_range
        self.num_total=num_total
        self.lista = lista

    def generate(self):
        list1=[]
        list2=[]
        lista = np.random.choice(range(10*self.num_range),size=self.num_range,replace = False).tolist()
        for i in lista:
            list1.append(str(i))
        list2=np.random.choice(list1, size=self.num_total-self.num_range, replace=True).tolist()
        list1.extend(list2)
        random.shuffle(list1)
#         print(list1)
        return list1

    def hash_process(self, iterator):
        result=[]
        for item in iterator:
            s1=self.hashfunc()
            s1.update(item.encode())
            res=s1.hexdigest()
            result.append(bin(int(res, 16))[6:])
        return result

    def seperate_to_each_bucket(self, iterator):
        for item in iterator:
            bucket_id, value=item[:self.b], item[self.b:]
            for i in range(len(value)):
                if value[i]=='1':
                    if self.bucket[bucket_id]<i+1:
                        self.bucket[bucket_id]=i+1
                    break
                else:
                    continue
    def hhl_compute_cardinality(self):
        sums=0
        #m=0
        for k, v in self.bucket.items():
            sums+=pow(2, -v)
            #m+=1
        return (1/sums)*pow(2**self.b, 2)*(0.7213 / (1.0 + 1.079 /2**self.b ))

    def hll(self):
        samples=self.generate()
        hashed_samples=self.hash_process(samples)
        self.seperate_to_each_bucket(hashed_samples)
        return int(self.hhl_compute_cardinality())

    def hll_computewithlist(self):
        hashed_samples = self.hash_process(self.lista)
        self.seperate_to_each_bucket(hashed_samples)
        return int(self.hhl_compute_cardinality())

def hll_computewithlisttotal(lista,num_range, num_total,b=8):
    test=hll_demo(num_range=num_range, num_total=num_total,b=b,lista=lista)
    yield test.hll_computewithlist()

"""
test=hll_demo(num_range = 500000, num_total = 4000000,b=10)
print(f'in our first example, for num_range = 500000, total number 4000000, the cardinality is {test.hll()}')
"""
sc = pyspark.SparkContext(appName="Project")
test = hll_demo(num_range = 500000,num_total = 9000000,b = 8)
lista = test.generate()
test = hll_demo(num_range = 500000,num_total = 9000000,b = 8,lista=lista)
start = time.time()
a = test.hll_computewithlist()
print(f'the total time for on one core is {time.time()-start}')
print(f'the cardinality value is {a} for num_range 500000, total number is 9000000')

rdd = sc.parallelize(lista, 4)
start = time.time()
mapvalue = rdd.mapPartitions(lambda sublist:hll_computewithlisttotal(lista=sublist,num_range=10,num_total=20,b=10))
print(f'we collect {mapvalue.collect()} in 4 parallization in spark')
print(f' we spend {time.time()-start}')
sc.stop()
