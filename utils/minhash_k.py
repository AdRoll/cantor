#!/usr/bin/python

import argparse
import sys

from scipy.stats import binom

def err(ci, k, j):
    n = binom.ppf(ci, k, j)
    if n == 0:
        #this is an edge case, so we report a big error
        return 1e9
    else:
        return abs(n/(j*k) - 1)

def find_k(j, alpha, conf, k=1, maxk=1000000):
    ci = 1 - ((1 - conf)/2.0)
    e = err(ci, maxk, j)
    if e > alpha:
        #we'll never get the precision we want for this
        #range, so output the end of the range
        return (maxk, e)
    #grab bounds for search space
    kn = find_bound(j, alpha, ci, k, maxk)
    ub = find_bound(j, 0.75*alpha, ci, k, maxk)
    #start searching...
    while True:
        if kn == maxk:
            break
        broken = False
        if err(ci, kn, j) <= alpha:
            for n in range(kn, min(2*kn, ub) + 1):
                if err(ci, n, j) > alpha:
                    kn = n + 1
                    broken = True
                    break
            if not broken:
                return (kn, err(ci, kn, j))
        else:
            kn += 1
    return (maxk, err(ci, maxk, j))

def find_bound(j, alpha, ci, k, maxk):
    #just a binary search to find good a good bound
    minb = k
    maxb = maxk
    e = err(ci, maxk, j)
    while True:
        midb = int((maxb + minb)/2)
        if midb - minb < 1:
            break
        midv = err(ci, midb, j)
        if midv <= alpha:
            maxb = midb
        else:
            minb = midb
    return midb

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Find an acceptable MinHash k given a desired error with desired confidence ' +
                    'at a particular Jaccard Index. Returns the k and the maximal error at that k.')
    parser.add_argument('--jaccard',
                        dest='jaccard_index',
                        required=True,
                        type=float,
                        help=('The lowest Jaccard Index to measure. In [0, 1].'))
    parser.add_argument('--error',
                        dest='error',
                        required=True,
                        type=float,
                        help=('The maximum error to tolerate at the Jaccard Index. ' + 
                              '1 implies a measurement of 0 or twice the actual Jaccard Index.'))
    parser.add_argument('--confidence',
                        dest='confidence',
                        required=True,
                        type=float,
                        help=('The level of confidence the error at the Jaccard Index ' +
                              'will be less than the maximum error.'))
    parser.add_argument('--min_k',
                        dest='min_k',
                        required=False,
                        type=int,
                        default=1,
                        help=('The smallest k at which to begin the search. Default is 1.'))
    parser.add_argument('--max_k',
                        dest='max_k',
                        required=False,
                        type=int,
                        default=1000000,
                        help=('The largest k which is acceptable. Default is 1e6.'))
    args = parser.parse_args()
    k, e = find_k(args.jaccard_index, args.error, args.confidence, k=args.min_k, maxk=args.max_k)
    print 'MinHash k:\t' + str(k)
    print 'Error at k:\t' + str(e)
