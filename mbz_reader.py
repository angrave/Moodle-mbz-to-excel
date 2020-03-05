#!/usr/bin/env python
# coding: utf-8

# # MBZ-XML-TO-EXCEL
# 
# 
# First pubished version May 22, 2019.  
# This is version 0.0006 (revision March 3, 2019)
# 
# Licensed under the NCSA Open source license
# Copyright (c) 2019,2020 Lawrence Angrave
# All rights reserved.
# 
# Developed by: Lawrence Angrave
#  
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal with the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# 
#    Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimers.
#    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimers in the documentation and/or other materials provided with the distribution.
#    Neither the names of Lawrence Angrave, University of Illinois nor the names of its contributors may be used to endorse or promote products derived from this Software without specific prior written permission. 
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH THE SOFTWARE. 
# 
# # Citations and acknowledgements welcomed!
# 
# In a presentation, report or paper please recognise and acknowledge the the use of this software.
# Please contact angrave@illinois.edu for a Bibliography citation. For presentations, the following is sufficient
# 
# MBZ-XML-TO-EXCEL (https://github.com/angrave/Moodle-mbz-to-excel) by Lawrence Angrave.
# MBZ-XML-TO-EXCEL is an iLearn project, supported by an Institute of Education Sciences Award R305A180211
# 
# If also using Geo-IP data, please cite IP2Location. For example,
# "This report uses geo-ip location data from IP2Location.com"
# 
# # Known limitations and issues
# 
# The assessment sheet (generated from workshop.xml) may generate URLs that are longer than 255 characters, 
# the largested supported by Excel. These very long URLs will be excluded
# 
# No verification of the data has been performed. 
# 
# It is unknown if the inferred timestamps based on the Unix Epoch timestamp require a timezone adjustment.
# 
# # Requirements
# 
# This project uses Python3 and Pandas and also the following additional modules-

# Example use:
''' Example use:
from mbz_reader import *
def main():
    mbz_files = ['backup-moodle2-course-20191234.mbz']
    for mbz in mbz_files:
        o = MBZ_Extractor_Config()
        o.geoip_datadir = 'geoip'

        o.archive_source_file = os.path.join('..','data',mbz)
        o.anonid_output_filename = 'usernames_anonids.csv' 

        o.generate_missing_anonid = 'salt+sha1'
        o.salt = 'secret-salt-horse-glass-'

        o.extract()

if __name__ == "__main__" : main()
'''

#pip install lxml
#pip install xlsxwriter
#pip install xlrd

import lxml
import xlsxwriter
import xlrd
import hashlib

#lxml supports line numbers
import lxml.etree as ET

from collections import OrderedDict
import pandas as pd
import numpy as np
import re
import os
import urllib
import datetime
import glob
import tarfile
import tempfile
import base64
# geoip support -
import bisect
import ipaddress
# timestamp support -
from datetime import datetime
# Extract text from html messages -
from bs4 import BeautifulSoup
import uuid
import traceback

import xlsxwriter
excelengine = 'xlsxwriter' 
# 'xlsxwriter' is currently recommended though it did not improve the write speed using generic pandas interface)
# Todo Perhaps using workbook interface directly will be faster? (https://xlsxwriter.readthedocs.io/)
# io.excel.xlsx.writer' (default, allegedly slow),
# 'pyexcelerate' (untested)


class MBZ_Extractor_Config:
    
    # # Load GeoIP data (optional)
    
    def load_geoip_data(self):
        self.geoip_all_colnames = ['geoip_ipfrom'
        ,'geoip_ipto'
        ,'geoip_country_code'
        ,'geoip_country_name'
        ,'geoip_region_name'
        ,'geoip_city_name'
        ,'geoip_latitude'
        ,'geoip_longitude'
        ,'geoip_zip_code'
        ,'geoip_time_zone']
    
        self.geoip_geo_columns = self.geoip_all_colnames[2:]
    
        #geoip_datadir = 'geoip' #change to your local directory of where the downloaded zip has been unpacked
        self.geoipv4_csv = os.path.join(self.geoip_datadir,'IP2LOCATION-LITE-DB11.CSV')
    
        if os.path.exists(self.geoipv4_csv):
            print("Reading geoip csv",self.geoipv4_csv)
            self.geoipv4_df = pd.read_csv(self.geoipv4_csv, names= self.geoip_all_colnames)
            self.geoipv4_ipvalues = self.geoipv4_df['geoip_ipfrom'].values
            # bisect searching assumes self.geoipv4_ipvalues are in increasing order 
        else:
            self.geoipv4_df = None
            self.geoipv4_ipvalues = None
            print("No GeoIP csv data at ",self.geoipv4_csv)
            print("IP addresses will not be converted into geographic locations")
            print("Free Geo-IP data can be downloaded from IP2LOCATION.com")
        
    # # Phase 1 - Extract XMLs from mbz file and create hundreds of Excel files
    
    # Each file can generate a list of tables (dataframes)
    # Recursively process each element. 
    # For each non-leaf element we build an ordered dictionary of key-value pairs and attach this to an array for the particular element name
    # <foo id='1' j='a'> becomes data['foo'] = [ {'id':'1', j:'a'} ]
    # The exception is for leaf elements (no-child elements) in the form e.g. <blah>123</blah>
    # We treat these equivalently to attributes on the surrounding (parent) xml element
    # <foo id='1'><blah>123</blah></foo> becomes data['foo'] = [ {'id':'1', 'blah':'123'} ]
    # and no data['blah'] is created
    
    AUTOMATIC_IMPLICIT_XML_COLUMNS = 4 #SOURCE_LINE,PARENT_SHEET,PARENT_INDEX
    
    def process_element(self, data,  tablename_list, context, e):
        #deprecated has_no_children = len(e.getchildren()) == 0
        has_no_children = len(e) == 0
        has_no_attribs = len(e.attrib.keys()) == 0
        text = e.text
            
        has_text = text is not None
        if has_text:
            text = text.strip()
            has_text = len(text) > 0
            
        # Is this a leaf element e.g. <blah>123</blah>
        # For the datasets we care about, leaves should not be tables; we only want their value   
        ignore_attribs_on_leaves = True
        
        # This could be refactored to return a dictionary, so multiple attributes can be attached to the parent
        if has_no_children and (has_no_attribs or ignore_attribs_on_leaves):
            if not has_no_attribs: 
                print()
                print("Warning: Ignoring attributes on leaf element:" + e.tag+ ":"+ str(e.attrib))
                print()
            return [e.tag,e.text] # Early return, attach the value to the parent (using the tag as the attribute name)
        
        table_name = e.tag
        if table_name not in data:
            tablename_list.append(table_name)
            data[table_name] = []
            
        key_value_pairs = OrderedDict()
        
        key_value_pairs['SOURCE_LINE'] = e.sourceline
        key_value_pairs['PARENT_SHEET'] = context[0]
        key_value_pairs['PARENT_ROW_INDEX'] = context[1]
        key_value_pairs['PARENT_ID'] = context[2]
        
        #print(e.sourceline)
        # For correctness child_context needs to be after this line and before recursion
        data[table_name].append(key_value_pairs)
        
        myid = ''
        if 'id' in e.attrib:
            myid = e.attrib['id']
            
        child_context = [table_name, len(data[table_name])-1, myid] # Used above context[0] during recursive call
        
        for key in sorted(e.attrib.keys()):
            key_value_pairs[key] = e.attrib[key]
            
        for child in e.iterchildren():
            # Could refactor here to use dictionary to enable multiple key-values from a discarded leaf
            key,value = self.process_element( data, tablename_list, child_context, child)
            if value:
                if key in key_value_pairs:
                    key_value_pairs[key] += ',' + str(value)
                else:
                    key_value_pairs[key] = str(value)
    
        
        if has_text:
            key_value_pairs['TEXT'] = e.text # If at least some non-whitespace text, then use original text
        
        return [e.tag,None]
    
    def tablename_to_sheetname(self, elided_sheetnames, tablename):
        sheetname = tablename
        # Future: There may be characters that are invalid. If so, remove them here..
    
        #Excel sheetnames are limited to 31 characters.
        max_excel_sheetname_length = 31
        if len(sheetname) <= max_excel_sheetname_length:
            return sheetname
        
        sheetname = sheetname[0:5] + '...' + sheetname[-20:]
        elided_sheetnames.append(sheetname)
        if elided_sheetnames.count(sheetname)>1:
            sheetname += str( elided_sheetnames.count(sheetname) + 1)
        
        return sheetname
    
    def decode_base64_to_latin1(self,encoded_val):
        try:
            return str(base64.b64decode(encoded_val) , 'latin-1')
        except Exception as e:
            traceback.print_exc()
            print("Not base64 latin1?", e)
            return '??Not-latin1 text'
    
    
    def decode_geoip(self,ip):
        try:
            ip = ip.strip()
            if not ip or self.geoipv4_df is None:
                return pd.Series(None, index=self.geoip_geo_columns)
            
            ipv4 = int(ipaddress.IPv4Address(ip))
            index = bisect.bisect(self.geoipv4_ipvalues, ipv4) - 1
            entry = self.geoipv4_df.iloc[index]
            assert entry.geoip_ipfrom  <= ipv4 and entry.geoip_ipto  >= ipv4
            return entry[2:] # [self.geoip_geo_columns] # Drop ip_from and ip_to
        except Exception as e:
            traceback.print_exc()
            print("Bad ip?",ip, e)
            return pd.Series(None, index=self.geoip_geo_columns)
    
    def decode_unixtimestamp_to_milliseconds(self,seconds):
        if seconds == '':
            return None
        try:
            return 1000. * float(seconds)
        except Exception as e:
            traceback.print_exc()
            print("Bad unix timestamp?", seconds , e)
            return None
    
    def decode_unixtimestamp_to_UTC(self,seconds):
        if seconds == '':
            return ''
        try:
            return datetime.utcfromtimestamp(int(seconds)).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            traceback.print_exc()
            print("Bad unix timestamp?", seconds , e)
            return ''
    
    def decode_html_to_text(self,html):
        if html is np.nan:
            return ''
        try:
            soup = BeautifulSoup(html,"lxml")
            return soup.get_text()
        except Exception as e:
            traceback.print_exc()
            print('Bad html?',html, e)
            return '???'
    
    def validate_anonid_data(self):
        #Expected columns
        for c in ['anonid','userid']:
            if c not in self.anonid_df.columns:
                raise ('self.anonid_input_filename\'' + self.anonid_input_filename + '\'should have a column named '+c)
                
        # No duplicate userid entries
        check_for_duplicates = self.anonid_df['userid'].duplicated(keep=False)
    
        if check_for_duplicates.any():
            print(self.anonid_df[check_for_duplicates])
            raise Exception('See above - fix the duplicates userid entries found in \'' + self.anonid_input_filename +'\'')
        
        self.anonid_df['userid'] = self.anonid_df['userid'].astype(str)
       
    def userid_to_anonid(self, moodleid):
        if moodleid is np.nan or len(moodleid) == 0:
            return ''
        try:
            username = self.moodleuser_to_username[moodleid] 
            row = self.anonid_df[ self.anonid_df['userid'] == username]
        except Exception as ex:
            print("**** Unknown moodle user number:", moodleid)
            return ''

        if len( row ) == 1:
            return str(row['anonid'].values[0])
        
        if self.generate_missing_anonid == 'uuid4':    
            result = uuid.uuid4().hex
        elif self.generate_missing_anonid == 'salt+sha1':
           if self.salt is None or len(self.salt) ==0:
              result = 'anonymized'
           else:
              result =  'p' + hashlib.sha1((self.salt + username).encode('utf-8')).hexdigest()[0:12]
        elif self.generate_missing_anonid is None:
            return ''
        else:
            raise ("self.generate_missing_anonid should be 'uuid4' or 'salt+sha1' or None")

        self.anonid_df = self.anonid_df.append({ 'userid':username, 'moodleid': str(moodleid), 'anonid':result}, ignore_index=True)
            
        return result
    
    def to_dataframe(self, table_name, table_data):
        df = pd.DataFrame(table_data)
        # Moodle dumps use $@NULL@$ for nulls
        df.replace('$@NULL@$','',inplace = True)
        
        # We found two base64 encoded columns in Moodle data-
        for col in df.columns & ['other','configdata']:
            df[ str(col) + '_base64'] = df[str(col)].map(self.decode_base64_to_latin1)
        
        for col in df.columns & ['timestart','timefinish','added','backup_date','original_course_startdate','original_course_enddate','timeadded','firstaccess','lastaccess','lastlogin','currentlogin','timecreated','timemodified','created','modified']:
            df[ str(col) + '_utc'] = df[str(col)].map(self.decode_unixtimestamp_to_UTC)
            if self.millisecond_times:
                 df[ str(col) + '_ms'] = df[str(col)].map(self.decode_unixtimestamp_to_milliseconds)
        
        # Extract text from html content
        for col in df.columns & ['message', 'description','commenttext','intro','conclusion','summary','feedbacktext','content','feedback','info', 'questiontext' , 'answertext']:
            df[ str(col) + '_text'] = df[str(col)].map(self.decode_html_to_text)
        
        # Moodle data has 'ip' and 'lastip' that are ipv4 dotted
        # Currently only ipv4 is implemented. self.geoipv4_df is None if the cvs file was not found
    
        if self.geoipv4_df is None:
            for col in df.columns & ['ip','lastip']:
                df = df.join( df[str(col)].apply(self.decode_geoip) )
    
        for col in df.columns & ['userid','relateduserid' , 'realuserid']:
            col=str(col)
            if col == 'userid':
                out = 'anonid'
            else: 
                out = col[0:-6] + '_anonid'
            df[ out ] = df[col].map(self.userid_to_anonid)
            if self.delete_userids:
                df.drop(columns=[col],inplace=True)
                
        if table_name == 'user':
            df['anonid'] = df['id'].map(self.userid_to_anonid)
            
        # Can add more MOODLE PROCESSING HERE :-)
        return df
        
    
    def to_absolute_file_url(self, filepath):
        return urllib.parse.urljoin( 'file:', urllib.request.pathname2url(os.path.abspath(filepath)))
    
    def write_excel_sheets(self, source_file, excelwriter, data, tablename_list):   
        elided_sheetnames = []
        table_sheet_mapping = dict()
        table_sheet_mapping[''] = '' # Top level parents have empty PARENT_SHEET
        
        for tablename in tablename_list:
            sheetname = self.tablename_to_sheetname(elided_sheetnames, tablename)
            table_sheet_mapping[tablename] = sheetname
        print('tablename_list:',tablename_list) 
        for tablename in tablename_list:
            # Either this is teh user table or we've already process the user table
            if tablename == 'user' and len(data['user'])>0 and ('username' in data['user'][0].keys()):
                assert( self.moodleuser_to_username is None)
                self.moodleuser_to_username = dict()
                for onerow in data[ tablename ]:
                    self.moodleuser_to_username[ onerow['id'] ] = onerow['username'] 
            else:
                assert( self.moodleuser_to_username is not None)
            df = self.to_dataframe(tablename, data[tablename])
            #Convert table (=original xml tag) into real sheet name (not tag name)
            if 'PARENT_SHEET' in df.columns:
                df['PARENT_SHEET'] = df['PARENT_SHEET'].apply(lambda x: table_sheet_mapping[x])
                
            df.index.rename(tablename, inplace=True)
            df.insert(0, 'SOURCE_FILE',source_file ,allow_duplicates=True)
            df.insert(1, 'SOURCE_TAG', tablename, allow_duplicates=True)
            sheetname = table_sheet_mapping[tablename]
            
            if sheetname != tablename:
                print("Writing "+ tablename + " as sheet "+ sheetname)
            else:
                print("Writing sheet "+ sheetname)
            
            df.to_excel(excelwriter, sheet_name=sheetname, index_label=tablename)
        return table_sheet_mapping
    
    
    
    def re_adopt_child_table(self, data, parent_tablename, parent_table, child_tablename):
        child_table = data[child_tablename]
        for row in child_table:
            if 'PARENT_SHEET' not in row.keys():
                continue
            if row['PARENT_SHEET'] == parent_tablename:
                idx = row['PARENT_ROW_INDEX']
                # Time to follow the pointer
                parent_row = parent_table[idx]
                #row['PARENT_TAG'] = parent_row['PARENT_TAG']
                row['PARENT_ROW_INDEX'] = parent_row['PARENT_ROW_INDEX']
                row['PARENT_ID'] = parent_row['PARENT_ID']
                row['PARENT_SHEET'] = parent_row['PARENT_SHEET']
        
        
    def discard_empty_tables(self, data,tablename_list):
        nonempty_tables = []
        for tablename in tablename_list:
            table = data[tablename]
            # print(tablename, len(table),'rows')
            if len(table) == 0:
                # print("Skipping empty table",tablename)
                continue
                
            include = False
            for row in table:
                if len(row) > self.AUTOMATIC_IMPLICIT_XML_COLUMNS: # Found more than just PARENT_TAG,... columns
                    include = True
                    break
            
            if include:
                # print("Including",tablename)
                nonempty_tables.append(tablename)
            else:
                # print("Skipping unnecessary table",tablename)
                # Will need to fixup child items that still think this is their container
                # More efficient if we kept a mapping of child tables, rather than iterate over tables
                for childname in tablename_list:
                    self.re_adopt_child_table(data, tablename, table, childname)
                pass
    
        return nonempty_tables
    
    #self.output_directory, relative_sub_dir, os.path.join(xml_dir,filename)    
    def process_one_file(self,  relative_sub_dir, xml_filename):
        print('process_one_file(\''+self.output_directory+'\',\''+relative_sub_dir+'\',\''+xml_filename+'\')')
        #print("Reading XML " + xml_filename)
        #Original parser 
        xmlroot = ET.parse(xml_filename).getroot()
        # Use lxml
        #xmlroot = etree.parse(xml_filename)
            
        data = dict()
        tablename_list = []
        
        initial_context = ['','',''] # Todo : Consider missing integer index e.g. ['',None,'']
        self.process_element(data, tablename_list, initial_context, xmlroot)
        
        nonempty_tables = self.discard_empty_tables(data,tablename_list)
        
        if len(nonempty_tables) == 0:
            #print("no tables left to write")
            return
        
        # We use underscore to collate source subdirectories
        basename = os.path.basename(xml_filename).replace('.xml','').replace('_','')
        
        use_sub_dirs = False
        if use_sub_dirs:
            output_dir = os.path.join(self.output_directory, relative_sub_dir)
    
            if not os.path.exists(output_dir): 
                os.mkdirs(output_dir)
    
            output_filename = os.path.join(output_dir,  basename + '.xlsx')
        else:
            sub = relative_sub_dir.replace(os.sep,'_').replace('.','')
            if (len(sub) > 0) and sub[-1] != '_':
                sub = sub + '_'
            output_filename = os.path.join(self.output_directory,  sub +  basename + '.xlsx')
        
        if self.dry_run: # For debugging
            return
        
        print("** Writing ", output_filename)
    
        if os.path.exists(output_filename):
            os.remove(output_filename)
           
        excelwriter = pd.ExcelWriter(output_filename, engine= excelengine)
            
        # absolute path is useful to open original files on local machine
        if(False):
            source_file = to_absolute_file_url(xml_filename)
        else:
            source_file = os.path.normpath(xml_filename)
            
        try:
            self.write_excel_sheets(source_file, excelwriter, data,nonempty_tables)
            excelwriter.close()
        except Exception as ex:
            traceback.print_exc()
            print(type(ex))
            print(ex)
            pass
        finally:
            
            excelwriter = None
        print()

    def process_directory(self, relative_sub_dir):
        xml_dir = os.path.join(self.expanded_archive_directory, relative_sub_dir)
        # We want to process the users.xml first
        file_list = sorted(os.listdir(xml_dir), reverse = True)
         
        for filename in file_list:
            if filename.endswith('.xml'):
                print("Processing", filename)
                assert((self.moodleuser_to_username is not None) or filename.endswith('users.xml'))
                self.process_one_file( relative_sub_dir, os.path.join(xml_dir,filename))
        
        if self.toplevel_xml_only:
            return # No recursion into subdirs(e.g. for testing)
        
        # Recurse
        for filename in file_list:
            candidate_sub_dir = os.path.join(relative_sub_dir, filename)
            if os.path.isdir( os.path.join(self.expanded_archive_directory, candidate_sub_dir)) :   
                self.process_directory(candidate_sub_dir)
    
    
    def extract_xml_files_in_tar(self, tar_file, extract_dir):
        os.makedirs(extract_dir)
        extract_count = 0
        for tarinfo in tar_file:
            if os.path.splitext(tarinfo.name)[1] == ".xml":
                #print(extract_dir, tarinfo.name)
                tar_file.extract( tarinfo, path = extract_dir)
                extract_count = extract_count + 1
        return extract_count
                
    def archive_file_to_output_dir(self,archive_file):
        return os.path.splitext(archive_file)[0] + '-out'
    
    def archive_file_to_xml_dir(self, archive_file):
        return os.path.splitext(archive_file)[0] + '-xml'
        
    def lazy_extract_mbz(self):
        has_xml_files = len( glob.glob( os.path.join(self.expanded_archive_directory,'*.xml') ) ) > 0
        
        if has_xml_files and self.skip_expanding_if_xml_files_found:
            print("*** Reusing existing xml files in", self.expanded_archive_directory)
            return
        
        if os.path.isdir(self.expanded_archive_directory):
            print("*** Deleting existing files in", self.expanded_archive_directory)
            raise "Comment out this line if it is going to delete the correct directory"
            shutil.rmtree(self.expanded_archive_directory)
            
        with tarfile.open(self.archive_source_file, mode='r|*') as tf:
            print("*** Expanding",self.archive_source_file, "to", self.expanded_archive_directory)
            extract_count = self.extract_xml_files_in_tar(tf, self.expanded_archive_directory)
            print('***',extract_count,' xml files extracted')



    def process_xml_files(self):
        
        print("*** Source xml directory :", self.expanded_archive_directory)
        print("*** Output directory:", self.output_directory)
    
        if not os.path.isdir(self.output_directory): 
            os.makedirs(self.output_directory)
        # We need the id-> username mapping
        self.moodleuser_to_username = None
        self.process_directory('.')
        
        if self.anonid_output_filename:
            filepath = os.path.join(self.output_directory, self.anonid_output_filename)
            print("Writing ",filepath,len(self.anonid_df.index),'rows')
            self.anonid_df.to_csv( filepath, index = None, header=True)
        
        print("*** Finished processing XML")
    
    
    # # Phase 2 - Aggregate Excel documents
    
    
    def list_xlsx_files_in_dir(self, xlsx_dir):
        xlsx_files = sorted(glob.glob(os.path.join(xlsx_dir,'*.xlsx')))
        xlsx_files = [file for file in xlsx_files if os.path.basename(file)[0] != '~' ]
        return xlsx_files
    
    # Phase 2 - Aggregate multiple xlsx that are split across multiple course sections into a single Excel file
    def create_aggregate_sections_map(self, xlsx_dir):
        xlsx_files = self.list_xlsx_files_in_dir(xlsx_dir)
        
        sections_map = dict()
    
        for source_file in xlsx_files:
            path = source_file.split(os.path.sep)  
            nameparts = path[-1].split('_')
            target = nameparts[:]
            subnumber = None
            if len(nameparts)>3 and nameparts[-3].isdigit(): subnumber = -3 # probably unnecessary as _ are removed from basename
            if len(nameparts)>2 and nameparts[-2].isdigit(): subnumber = -2
            if not subnumber: continue
    
            target[subnumber] = 'ALLSECTIONS'
    
            key = (os.path.sep.join(path[:-1]))  + os.path.sep+ ( '_'.join(target))
            if key not in sections_map.keys():
                sections_map[key] = []
            sections_map[key].append(source_file)
        return sections_map
    
    # Phase 3 - Aggregate over common objects
    def create_aggregate_common_objects_map(self, xlsx_dir):
        xlsx_files = self.list_xlsx_files_in_dir(xlsx_dir)
        
        combined_map = dict()
        # path/_activities_workshop_ALLSECTIONS_logstores.xlsx will map to key=logstores.xlsx
        for source_file in xlsx_files:
            path = source_file.split(os.path.sep) 
            nameparts = path[-1].split('_')
            target = nameparts[-1]
    
            if 'ALL_' == path[-1][:4]:
                continue # Guard against restarts
    
            key = (os.path.sep.join(path[:-1])) + os.path.sep+ ('ALL_' + target)
            if key not in combined_map.keys():
                combined_map[key] = []
            combined_map[key].append(source_file)
    
        return combined_map   
    
    
    def rebase_row(self, row,rebase_map):
        if isinstance(row['PARENT_SHEET'] , str):     
            return str(int(row['PARENT_ROW_INDEX']) + int(rebase_map[ row['XLSX_SOURCEFILE'] + '#' + row['PARENT_SHEET'] ]))
        else:
            return ''
    
    
    def check_no_open_Excel_documents_in_Excel(self):
        # Excel creates temporary backup files that start with tilde when an Excel file is open in Excel
        if not os.path.isdir(self.output_directory):
            return
        open_files = glob.glob(os.path.join(self.output_directory,'~*.xlsx'))
        if len(open_files):
            print( 'Please close ' + '\n'.join(open_files) + '\nin directory\n'+dir)
            raise IOError('Excel files '+('\n'.join(open_files))+' are currently open in Excel')
        
    def aggregate_multiple_excel_files(self,source_filenames):
        allsheets = OrderedDict()
        rebase_map = {}
        # !! Poor sort  - it assumes the integers are the same char length. Todo improve so that filename_5_ < filename_10_  
        for filename in sorted(source_filenames):
            print('Reading and aggregating sheets in' , filename)
            xl = pd.ExcelFile(filename)
            for sheet in xl.sheet_names:
                
                df = xl.parse(sheet)
                df['XLSX_SOURCEFILE'] = filename
                if sheet not in allsheets.keys():
                    allsheets[sheet] = df
                    rebase_map[filename+'#'+sheet] = 0
                else:
                    row_offset =  len(allsheets[sheet]) 
                    rebase_map[filename+'#'+sheet] = row_offset # We will need this to rebase parent values
                    df[ df.columns[0] ] += row_offset
                    allsheets[sheet] = allsheets[sheet].append(df, ignore_index =True, sort = False)
            xl.close()
            
        # print('rebase_map',rebase_map)
        # The row index of the parent no longer starts at zero
        print('Rebasing parent index entries in all sheets')    
        for sheet in xl.sheet_names:
            df = allsheets[sheet]       
            df['PARENT_ROW_INDEX'] = df.apply( lambda row: self.rebase_row( row,rebase_map), axis = 1)
            df.drop('XLSX_SOURCEFILE', axis = 1, inplace = True)
        return allsheets
    
    def write_aggregated_model(self, output_filename, allsheets):
        print("Writing",output_filename)
        if self.dry_run:
            print("Dry run. Skipping ", allsheets.keys())
            return
        
        excelwriter = pd.ExcelWriter(output_filename, engine = excelengine)
        try:
            print("Writing Sheets ", allsheets.keys())
            for sheetname,df in allsheets.items():
                df.to_excel(excelwriter, sheet_name = sheetname, index = 'INDEX')
            excelwriter.close()
            
        except Exception as ex:
            print(type(ex))
            print(ex)
            pass
        
        finally:
            excelwriter.close()
            print('Writing finished\n')
    
    def move_old_files(self, xlsx_dir, filemap, subdirname):
        xlsxpartsdir = os.path.join(xlsx_dir,subdirname)
        if self.dry_run:
            print('Dry run. Skipping move_old_files', filemap.items(),' to ', subdirname)
            return
        
        if not os.path.isdir(xlsxpartsdir): 
            os.mkdir(xlsxpartsdir)
    
        for targetfile,sources in filemap.items():
            for file in sources:
    
                dest=os.path.join(xlsxpartsdir, os.path.basename(file))
                print(dest)
                os.rename(file, dest)
    
    def aggreate_over_sections(self):
        sections_map= self.create_aggregate_sections_map(self.output_directory)
    
        for targetfile,sources in sections_map.items():
            allsheets = self.aggregate_multiple_excel_files(sources)
            self.write_aggregated_model(targetfile, allsheets)
    
        self.move_old_files(self.output_directory, sections_map,'_EACH_SECTION_')
    
    def aggreate_over_common_objects(self):
        combined_map = self.create_aggregate_common_objects_map(self.output_directory)
        
        for targetfile,sources in combined_map.items():
            allsheets = self.aggregate_multiple_excel_files(sources)
            self.write_aggregated_model(targetfile, allsheets )
            
        self.move_old_files(self.output_directory, combined_map, '_ALL_SECTIONS_' )
    
    def create_column_metalist(self):
        xlsx_files = self.list_xlsx_files_in_dir(self.output_directory)
        
        metalist = []
    
        for filename in xlsx_files:
            print(filename)
            xl = pd.ExcelFile(filename)
            filename_local = os.path.basename(filename)
    
            for sheet in xl.sheet_names:
    
                df = xl.parse(sheet,nrows=1)
    
                for column_name in df.columns:
                    metalist.append([filename_local,sheet,column_name])
            xl.close()
    
        meta_df = pd.DataFrame(metalist, columns=['file','sheet','column'])
    
        meta_filename = os.path.join(self.output_directory,'__All_COLUMNS.csv')
        if self.dry_run:
            print('Dry run. Skipping',meta_filename)
        else:
            meta_df.to_csv(meta_filename,sep='\t',index=False)
    

    def extract(self):     
        # Some typical numbers:
        # A 400 student 15 week course with  16 sections
        # Created a 4GB mbz which expanded to 367 MB of xml. (the non-xml files were not extracted)
        # 30 total minutes processing time: 15 minutes to process xml, 
        # 6   minutes for each aggegration step, 2 minutes for the column summary
        # Final output: 60MB of 'ALL_' Excel 29 files (largest: ALL_quiz.xlsx 35MB, ALL_logstores 10MB, ALL_forum 5MB)
        # The initial section output (moved to _EACH_SECTION_/) has 334 xlsx files, 
        # which is futher reduced (see _ALL_SECTIONS_ ) 67 files.
        if not self.archive_source_file and not self.expanded_archive_directory:
            raise ValueError('Nothing to do: No mbz archive file or archive directory (with .xml files) specified')
        
        if self.archive_source_file and not os.path.isfile(self.archive_source_file) :
            raise ValueError('self.archive_source_file (' + os.path.abspath(self.archive_source_file) + ") does not refer to an existing archive")
        
        if not self.expanded_archive_directory:
            self.expanded_archive_directory = self.archive_file_to_xml_dir(self.archive_source_file)
        
        if not self.output_directory:
            if self.archive_source_file:
                self.output_directory = self.archive_file_to_output_dir(self.archive_source_file)
            else:
                raise ValueError('Please specify self.output_directory')
            
        if self.anonid_input_filename:
            print ('Reading' + self.anonid_input_filename + ' mapping')
            self.anonid_df = pd.read_csv(self.anonid_input_filename)
            
            self.validate_anonid_data()
        else:
            self.anonid_df = pd.DataFrame(columns=['userid','moodleid','anonid']) # 'userid':'-1','anonid':'example1234'}])
        
        
        start_time = datetime.now()
        print(start_time)
        
        if self.geoip_datadir :
            self.load_geoip_data()
            
        if self.archive_source_file:
            self.lazy_extract_mbz()
        
        self.check_no_open_Excel_documents_in_Excel()
        # Now the actual processing can begin
        
        self.process_xml_files() #self.expanded_archive_directory,self.output_directory, self.toplevel_xml_only, self.dry_run, self.anonid_output_filename)
        # At this point we have 100s of Excel documents (one per xml file), each with several sheets (~ one per xml tag)!
        # We can aggregate over all of the course sections
        self.aggreate_over_sections() #self.output_directory)
        
        # Workshops, assignments etc have a similar structure, so we also aggregate over similar top-level objects
        self.aggreate_over_common_objects()# self.output_directory)
        self.create_column_metalist()
        
        end_time = datetime.now()
        print(end_time)
        print(end_time-start_time)

    def __init__(self) :
        self.archive_source_file = None

        self.expanded_archive_directory = None

        self.skip_expanding_if_xml_files_found = True

        # Defaults to sibling directory "-out"
        self.output_directory = None

        self.generate_missing_anonid = 'uuid4'  # uuid4 'salt+sha1' or NOne

        self.geoip_datadir = None

        self.anonid_input_filename = None
        # A simple csv file with header 'userid','anonid'

        self.anonid_output_filename='userids_anonids.csv' # None if mapping should not be written

        self.delete_userids = False # User table will still have an 'id' column
        #relateduserids,realuserid andu userid columns in other tables are dropped
        self.millisecond_times = True

        # Internal testing options
        self.toplevel_xml_only = False # Don't process subdirectories. Occasionally useful for internal testing
        self.dry_run = False # Don't write Excel files. Occasionally useful for internal testing

