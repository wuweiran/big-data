from __future__ import print_function

import sys
from pyspark import SparkContext
from datetime import datetime


def identify_type(value):
    try:
        converted = int(value)
        return 'INTEGER'
    except:
        pass
    try:
        converted = float(value)
        return 'REAL'
    except:
        pass
    try:
        converted = datetime.strptime(text, '%Y-%m-%d')
        return 'DATE/TIME'
    except:
        pass
    return 'TEXT'

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task1 <input-csv>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    input = sc.textFile(sys.argv[1], 1)
    headers = input.first().split('\t')
    data = input.zipWithIndex().filter(lambda tup: tup[1] > 1) \
                .map(lambda x: x[0]) \
                .map(lambda x: x.split('\t')) \
                .cache()
    result = sc.parallelize([])
    for i in range(len(headers)):
        column = headers[i]
        values = data.map(lambda x: x[i])
        nonEmptyNum = values.filter(lambda x: x.strip() != '').count()
        EmptyNum = values.filter(lambda x: x.strip() == '').count()
        distinctNum = values.distinct().count()
        topFiveFrequent = values.map(lambda x: (x, 1)) \
                                .reduceByKey(lambda v1, v2: v1 + v2) \
                                .sortBy(lambda x: -x[1]) \
                                .map(lambda x: x[0]) \
                                .take(5)
        dataTypes = values.map(lambda x: identify_type(x)).distinct().collect()
        extra = {}
        if 'INTEGER' in dataTypes or 'REAL' in dataTypes:
            values = values.filter(lambda x: identify_type(x) == 'INTEGER' or identify_type(x) == 'REAL') \
                           .map(lambda x: float(x))
            extra['max_number'] = values.max()
            extra['min_number'] = values.min()
            extra['mean'] = values.mean()
            extra['stdev'] = values.stdev()
        if 'DATE/TIME' in dataTypes:
            values = values.filter(lambda x: identify_type(x) == 'DATE/TIME') \
                           .map(lambda x: datetime.strptime(x, '%Y-%m-%d'))
            extra['max_date'] = values.max()
            extra['min_date'] = values.min()
        if 'TEXT' in dataTypes:
            values = values.filter(lambda x: identify_type(x) == 'TEXT') \
                           .map(lambda x: (x, len(x)))
            fiveLongest = values.sortBy(lambda x: -x[1]) \
                                .map(lambda x: x[0]) \
                                .take(5)
            fiveShortest = values.sortBy(lambda x: x[1]) \
                                 .map(lambda x: x[0]) \
                                 .take(5)
            extra['five_longest'] = fiveLongest
            extra['five_shortest'] = fiveShortest
        tuple = (column, nonEmptyNum, EmptyNum, distinctNum, topFiveFrequent, dataTypes, str(extra))
        result = result.union(sc.parallelize([tuple]))

    result.saveAsTextFile("task1.csv")

    sc.stop()
