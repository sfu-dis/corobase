#!/usr/bin/env python

# Desc: Reads the result file and loads all the entries to corresponding
#       vectors of ResultEntries, one vector per algo

# For 
import csv

# for renaming the output to .eps.in
import sys
import os

#from sys import argv
from os import rename, listdir

#########################################
# Class MyData
# Author: Ronald Barber rjbarber@us.ibm.com 4/7/2014

# Usage:
#    d = MyData("results/vldb_experiment.dat" [, header_str="timestamp"] [, header_map={"hostname":"host"}] [, delimiter=' '] [, quotechar='"'] );
#    print d.results['algo']
#    May use range(incl-start, excl-end) like range(1,17) to allow 1 to 16 threads
#    r = d.filter({"threads":range(1,17)}, exclude={"density":2})
#    print r['hostname']
#    print r['density']
#    print r['threads']

# Sample Input File:
#timestamp algo inner outer key-size payload-size density skew selectivity threads bthreads HT isCat logHT probe-time bld-time total-time htsize L1 maxrss hostname
#2014/04/04_13:46:04 cht 100 100000000 8 8 2 0.0 100 8 8 HT 0 11 1046.58 30.261 1076.84 528 MAXRSS 67976 loud25

class MyData:
    "Manage a set of results from CSV formatted file with header (can be repeated header with different column formats)"
    # rows is a dictionary with elents named column headers and values a list with entries
    # containing data values with consistent offsets across dictionary elements.
    #    results = {}  # Dictionary of results

    @staticmethod
    def MyInit(PyPlot):
        # Check if non-X needed
        if 'eps' in sys.argv or 'pdf' in sys.argv or 'pdf.in' in sys.argv:
            PyPlot.use('Agg')

    @staticmethod
    def MyShow(PyPlot):
        # Check if an eps argument to write to file instead of plotting
        if 'eps' in sys.argv:
            ## Save to file instead of plotting it
            fname = sys.argv[0].replace(".py",".eps")
            PyPlot.savefig(fname)
            ## Rename to eps.in
            targetFname = fname + ".in"
            if os.path.exists(targetFname):
                os.remove(targetFname)
            rename(fname,targetFname)
            print 'Wrote: ' + targetFname
        elif 'pdf.in' in sys.argv:
            ## Save to file instead of plotting it
            fname = sys.argv[0].replace(".py",".pdf")
            PyPlot.savefig(fname)
            ## Rename to eps.in
            targetFname = fname + ".in"
            if os.path.exists(targetFname):
                os.remove(targetFname)
            rename(fname,targetFname)
            print 'Wrote: ' + targetFname
        elif 'pdf' in sys.argv:
            fname = sys.argv[0].replace(".py",".pdf")
            PyPlot.savefig(fname)
            print 'Wrote: ' + fname
        else:
            PyPlot.show()

    @staticmethod
    def MyAvg(list):
        # if list is of numeric, casts to float and returns average
        # if list contains non-numeric then checks if all entries equal and gets ValueError if not
        # returns 0 if list is empty
        if not(hasattr(list, '__iter__')) or isinstance(list, basestring): #Homogenize to a list if not one
            list = [list]  # Make a list if not one
        if not(list): return 0.0
        elif len(list)==1:  # Special case avg that returns number as-is but as int or float or string
            try: return int(list[0])
            except Exception:
                pass
                try: return float(list[0])
                except Exception:
                    pass
                    return list[0]
        try:
            sum=0.0
            for v in list:
                sum = sum + float(v)
            return sum/len(list)
        except Exception:
            pass
            for v in list:
                if list[0] != v:
                    raise ValueError("Inconsistent List Value in: "+str(list))
            return list[0]

    def appendFile(self, filename, header_str='timestamp', header_map={}, delimiter=' ', quotechar='"'):
        "Reads the result file and load the entries into results dictionary as lists using column headers"

        # Open and read result file
        with open(filename,"r") as datfile:
            resultReader = csv.reader(datfile, delimiter=delimiter, quotechar= quotechar)
            header=[]  # will be last read header
            try:
                for row in resultReader:
                    if not(row): continue  # Ignore blank lines
                    try: # Check for new header row
                        #print header_str, " ", row, " ", row.index(header_str)
                        (row.index(header_str) >= 0)  # get's exception on not found
                        header = row
                        # Remap any mapping selections
                        if (header_map):
                            for i in range(0, len(header)):
                                if header_map.has_key(header[i]):
                                    header[i] = header_map[header[i]] # Replace with mapped value
                        continue
                    except: pass # NO-OP

                    if len(row) != len(header):
                        print "** IGNORING Row elements", len(row), "header elements", len(header), "on line", resultReader.line_num
                        print zip(header,row)
                        continue

                    # Copy all attributes (Pad lists as needed)
                    for i in range(0, len(row)):
                        v = self.results.setdefault(header[i], [])
                        v.extend([None]*(self.elements-len(v)+1))
                        v[self.elements] = row[i]
                    self.elements = self.elements+1

            except csv.Error as e:
                print "** Error line", resultReader.line_num, "from", filename, "reason", e
                raise ExceptionToThrow(e)

            # Pad all attributes to elements entries to simplify filtering
            for key, values in self.results.items():
                if self.elements > len(values):
                    values.extend([None]*(self.elements-len(values)))

            print "Read", self.elements, "data elements from", resultReader.line_num, "lines in file", filename

    def __init__(self, filename, header_str='timestamp', header_map={}, delimiter=' ', quotechar='"'):
        # Example header_map={"hostname":"host"}
        self.elements = 0
        self.results = {}  # Dictionary of results
        if filename: # Is filename not empty and not None
            if isinstance(filename, basestring):
                self.appendFile(filename, header_str, header_map, delimiter, quotechar)

    def filter(self, include={}, exclude={}):
        # Give warning on unknown ecluded items only once
        for key in exclude.keys():
            if not(self.results.has_key(key)): # No Data with eclude (NO-OP)
                print "NOTE: Exclude of item", key, "was not known in header:", self.results.keys()

        r = {}
        # Copy all attributes names
        for key, values in self.results.items():
            r.setdefault(key, [])
        e = 0
        for i in range(0, self.elements):
            # Check if included
            ok=True
            for key, values in include.items():
                if not(self.results.has_key(key)):
                    print "Filter by '"+key+"' not allowed as not occuring in any header. Known columns:", self.results.keys()
                    raise Exception("Invalid Filter")
                if hasattr(values,'__iter__') and not(isinstance(values, basestring)): # is a list vs single item?
                    for value in values:
                        if value == None and self.results[key][i] == None:
                            break # done searching
                        elif str(value) == self.results[key][i]:
                            break # done searching
                    else:
                        ok = False
                        break
                else:
                    if values == None and self.results[key][i] == None:
                        break # done searching
                    elif str(values) == self.results[key][i]:
                        continue
                    else:
                        ok = False
                        break
            if not(ok): # Skip Include Failures
                continue

            # Check if excluded
            for key, values in exclude.items():
                if not(self.results.has_key(key)): # No Data with eclude (NO-OP)
                    continue
                if hasattr(values,'__iter__') and not(isinstance(values, basestring)): # is a list vs single item?
                    for value in values:
                        if value == None and self.results[key][i] == None:
                            ok = False
                            break # done searching
                        elif str(value) == self.results[key][i]:
                            ok = False
                            break # done searching
                    else:
                        if not(ok): # Continue exiting on exclude
                            break
                else:
                    if values == None and self.results[key][i] == None:
                        ok = False
                        break # done searching
                    elif str(values) == self.results[key][i]:
                        ok = False
                        break
                    else:
                        continue
            if not(ok): # Skip Exclude Failures
                continue

            # Copy all attributes (Pad lists as needed)
            for key, values in self.results.items():
                v = r[key]
                v.extend([None]*(e-len(v)+1))
                if i < len(values): # Only copy available values
                    v[e] = values[i]
            e = e+1

        print "Filtered", self.elements, "data elements to", e, "elements"
        return r

    def filterSubset(self, include={}, exclude={}):
        # Pefrorms SELECT * WHERE include AND NOT(exclude) and returns new MyData Object
        r = self.filter(include, exclude)
        mydata = MyData(None)
        if r:
            mydata.results = r
            mydata.elements = len(r.values()[0])
        return mydata

    def filterSelect(self, xcol=None, ycol=None, include={}, exclude={}, xvalues=[]):
        # Pefrorms SELECT xcol, AVG(ycol) GROUP BY xcol WHERE include AND NOT(exclude)
        # Give warning on unknown ecluded items only once
        if not(self.results.has_key(xcol)):
            print "Filter select column unknown '"+xcol+"' not allowed as not occuring in any header. Known columns:", self.results.keys()
            raise Exception("Invalid Filter")
        if not(self.results.has_key(ycol)):
            print "Filter select column unknown '"+ycol+"' not allowed as not occuring in any header. Known columns:", self.results.keys()
            raise Exception("Invalid Filter")
        r = self.filter(include, exclude)
        agg={}
        for x,y in zip(r[xcol],r[ycol]):
            agg.setdefault(x,[])
            agg[x].append(y)
        if not(xvalues): # use data order and keys
            xvalues = agg.keys()
        if not(hasattr(xvalues, '__iter__')) or isinstance(xvalues, basestring): #Homogenize to a list if not one
            xvalues = [xvalues]  # Make a list if not one
        Xs, AVGs=[],[]
        for x in xvalues:
            if agg.has_key(str(x)): # Skip any missing X values
                Xs.append(MyData.MyAvg(x)) # Convert to Numeric if possible
                AVGs.append(MyData.MyAvg(agg[str(x)]))
                if len(agg[str(x)])>1:
                    print "Averaged for x=",x," Values: ",agg[str(x)]
            else:
                print "filterSelect: Skipping missing X value", x, "in available sequence",agg.keys()
        return Xs, AVGs

    def append(self, source):
        # Appends a MyData to the self MyData
        # Merge existing columns in source into self
        for key, values in source.results.items():
            if self.results.has_key(key): # Existing Column?
                v = self.results[key]
            else: # Pad self for new column in source
                v = self.results.setdefault(key, [])
                v.extend([None]*self.elements)
            v.extend(values) # Append new values

        self.elements = self.elements + source.elements

        # Pad all attributes to elements entries to simplify filtering
        for key, values in self.results.items():
            if self.elements > len(values):
                values.extend([None]*(self.elements-len(values)))
        print "append: Added", source.elements, "in available data now sized at",self.elements
