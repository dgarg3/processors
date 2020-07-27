# from functools import reduce
# x = [10,11,8,13]
# print(list(filter(lambda i: len(str(i)) < 2,x)))
#
# x = ['ABC','Shsh','djdjd']
#
# print(list(map(lambda s:s.upper(),x)))
#
# print(reduce(lambda i,x1:i+x1,x))

#t = lambda x,y : x+y
#print (t(2,3))

x = [{ "name":"abc","score":"def"},{ "name":"abc","score":"def"}]

y = list(map(lambda i : i['name'].upper(),x))

print(y)