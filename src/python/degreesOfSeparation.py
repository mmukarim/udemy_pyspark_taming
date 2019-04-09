from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)

startCharacter = 500
endCharacter = 200

hitCounter = sc.accumulator(0)

def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for field in fields[1:]:
        connections.append(int(field))
    distance = 9999
    color = 'WHITE'
    if heroID == startCharacter:
        distance = 0
        color = 'GRAY'
    return heroID, (connections, distance, color)

def createStartingRDD():
    inputFile = sc.textFile("./Marvel-Graph.txt")
    return inputFile.map(convertToBFS)

def bfsMap(node):
    connectionID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if color == 'GRAY':
        for connection in connections:
            newStartID = connection
            newDistance = distance+1
            newColor = 'GRAY'
            if endCharacter == connection:
                hitCounter.add(1)
                break
            results.append((newStartID, ([], newDistance, newColor)))
        color = 'BLACK'

    results.append((connectionID, data))
    return results

def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]
    edges = []

    if len(edges1) > 0:
        edges.extend(edges1)
    if len(edges2) > 0:
        edges.extend(edges2)

    distance = distance1 if distance1<distance2 else distance2
    color = 'WHITE'
    if color1=='BLACK' or color2 == 'BLACK':
        color = 'BLACK'
    elif color != 'BLACK' and (color1 == 'GRAY' or color2=='GRAY'):
        color = 'GRAY'
    return edges, distance, color

rdd = createStartingRDD()
i = 0
while(hitCounter.value <= 0):
    i+=1
    print('Iteration: ' + str(i) + '.')
    mapped = rdd.flatMap(bfsMap)
    print('Processing ' + str(mapped.count()) + ' values.')
    rdd = mapped.reduceByKey(bfsReduce)
print('Hit the target point from ' + str(hitCounter.value) + ' different directions.')

