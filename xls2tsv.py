#!/usr/bin/env python

import sys
import os

# add path to xlrd local libary...
scr = sys.argv[0]
scr = os.path.abspath(scr)
scr_list = scr.split('/')
trunk_pos = scr_list.index('trunk')
util_path = '/'.join(scr_list[:trunk_pos+1] + ['Python','util'])
util_path2 = '/'.join(scr_list[:trunk_pos+1] + ['Python','util','xlrd-0.6.1'])

sys.path.append(util_path)
sys.path.append(util_path2)



#scr = sys.argv[0]
#scr = os.path.abspath(scr)
#scr_path = os.path.dirname(scr)
#xlrd_path = os.path.join(scr_path,'xlrd-0.6.1')
#print xlrd_path
#sys.path.append(xlrd_path)

import csv
import os
import xlrd

def xls2tsv(xlsfile,tsvfile):

    if not os.path.exists(xlsfile):
        raise Exception("Input file %s does not exist."%xlsfile)
    
    if os.path.exists(tsvfile):
        pass #raise Exception("Output file %s already exists."%outfile)
    
    try:
        # The text conversion will fail if it hits a null character, as is typical in XLS files
        # The text conversion will also fail if the formatting is strange.
        text_excel_to_tsv(xlsfile,tsvfile)
    except:    
    
        # The excel conversion will fail if not in right binary format
        # xlrd.biffh.XLRDError: Expected BOF record; found 0x6557
        try:
            excel_to_tsv(xlsfile,tsvfile)    
        except:
            raise Exception ('File not in TSV or XLS format')

def excel_to_tsv(excel_name, tsv_name):
    book = xlrd.open_workbook(excel_name)
    outf = file(tsv_name, 'w')
    #print "The number of worksheets is", book.nsheets
    #print "Worksheet name(s):", book.sheet_names()
    sh = book.sheet_by_index(0)
    #print sh.name, sh.nrows, sh.ncols
##    print "Cell D30 is", sh.cell_value(rowx=29, colx=3)
    for rx in range(sh.nrows):
        output =  '\t'.join(map(lambda cell:str(cell.value), sh.row(rx))) + '\n'
        outf.write(output)
    outf.close()
    
def text_excel_to_tsv(infile,outfile):
    infid = open(infile,'rU')
    outfid = open(outfile,'w')

    in_reader = csv.reader(infid,dialect='excel-tab')
    out_writer = csv.writer(outfid,dialect='excel-tab',lineterminator='\n')

    for row in in_reader:
        out_writer.writerow(row)
    
    infid.close()
    outfid.close()

if __name__=='__main__':
    
    xlsfile = sys.argv[1]
    tsvfile = sys.argv[2]
    xls2tsv(xlsfile,tsvfile)
    
