#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 05:58:04 2020
@author: ashisood
"""
'''
#tshark -r wd.vwr -T fields -e frame.number -e frame.len -e frame.time -e frame.time_delta -e frame.time_epoch -e eth.type -e ip.id -e lanforge.seqno -e ixveriwave.sig_ts -E header=y -E separator=, -E quote=d >wd.vwr.csv
#tshark -r ws.vwr -T fields -e frame.number -e frame.len -e frame.time -e ixveriwave.frame_length -e wlan.fc.type -e radiotap.length -e ixveriwave.l1info.rate -e frame.time_epoch -e wlan.qos.amsdupresent -e wlan_radio.phy -e wlan_radio.11n.mcs_index -e wlan_radio.11ac.mcs -e wlan_radio.data_rate -e wlan_radio.signal_dbm -e wlan.fc.type -e wlan.fc.type_subtype -e wlan.fc.retry -e wlan.qos.tid -e wlan.qos.priority -e wlan.fc.tods -e wlan.fc.fromds -e wlan.bssid -e wlan.staa -e ixveriwave.mumask -e wlan.tag.number -e llc.type -e ip.id -e lanforge.seqno -e ixveriwave.sig_ts -E header=y -E separator=, -E quote=d >ws.vwr.csv

#tshark -r vid_wired_2.pcapng -T fields -e frame.number -e frame.len -e frame.time -e frame.time_delta -e frame.time_epoch -e eth.type -e ip.id -e lanforge.seqno -e ixveriwave.sig_ts -E header=y -E separator=, -E quote=d >vid_wired_2.pcapng.csv
#tshark -r vid_wireless_2.pcapng -T fields -e wlan_radio.data_rate -e wlan.frag -e ixveriwave.frame_length -e ixveriwave.mumask -e frame.number -e frame.len -e frame.time -e radiotap.length -e wlan.seq -e ixveriwave.l1info.rate -e frame.time_epoch -e wlan.qos.amsdupresent -e wlan_radio.phy -e wlan_radio.11n.mcs_index -e wlan_radio.11ac.mcs -e wlan_radio.signal_dbm -e wlan.fc.type -e wlan.fc.type_subtype -e wlan.fc.retry -e wlan.qos.tid -e wlan.qos.priority -e wlan.fc.tods -e wlan.fc.fromds -e wlan.bssid -e wlan.staa -e ixveriwave.mumask -e wlan.tag.number -e llc.type -e ip.id -e lanforge.seqno -e ixveriwave.sig_ts -E header=y -E separator=, -E quote=d >vid_wireless_2.pcapng.csv
'''

import pandas as pd
import warnings
from pandas.core.common import SettingWithCopyWarning
import os
import subprocess
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import argparse
import configparser
import logging
from inspect import currentframe
import pathlib
is_candela = False
is_veriwave = False

def get_linenumber():
    cf = currentframe()
    return cf.f_back.f_lineno

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
warnings.simplefilter(action='ignore', category=FutureWarning) 
latency_file = "latency.csv"
airtime_file = "airtime.csv"
rate_file = "ratenmcs.csv"
bssid_string = []

ws_cols = ["frame.number",
           "frame.time",
           "frame.len",
           "radiotap.length",
           "ixveriwave.mumask",
           "wlan_radio.phy",
           "wlan_radio.11n.mcs_index",
           "wlan.seq",
           "wlan_radio.11ac.mcs",
           "wlan_radio.signal_dbm",
           "ixveriwave.frame_length",
           "wlan.fc.type",
           "wlan.fc.type_subtype",
           "wlan.qos.tid",
           "wlan.ra",
           "wlan.ta",           
           "wlan.qos.priority",
           "radiotap.ampdu.reference",
           "wlan.fc.tods",
           "wlan.fc.fromds",
           "wlan.bssid",
           "wlan.staa",
           'wlan.frag',
           "wlan.tag.number",
           "wlan.da",
           "wlan_radio.data_rate",
           "wlan.fc.retry",
           "ixveriwave.l1info.rate",
           "frame.time_epoch", 
           "llc.type",
           "ixveriwave.sig_ts",          
           "lanforge.seqno",
           "radiotap.he.data_3.data_mcs",
           "radiotap.he.data_5.gi",
           "radiotap.he.data_5.data_bw_ru_allocation",
           "radiotap.he.data_6.nsts"]
      
ixvw_hdr_len = 87
fcs= 4

airtime_cols = ['T-airtime',
                'T-Bytes',
                'T-Retry-Bytes',
                'T-MSDU',
                'T-MPDU',
                'T-PPDU',
                'T-Retry-MSDU',
                'T-Retry-MPDU',
                'R-airtime',
                'R-Bytes',                
                'R-MSDU',
                'R-MPDU',
                'R-PPDU',
                'T-b/ms',
                'R-b/ms']

rate_cols =     ['R-MSDU',
                'R-MPDU',
                'R-PPDU',
                'T-MSDU',
                'T-MPDU',
                'T-PPDU',                
                'R-Retry-MSDU',
                'T-Retry-MSDU',
                'R-b/ms',
                'T-b/ms']

latency_cols = ['latency in us', 
                'WiredFrameNo', 
                'WirelessFrameNo', 
                'Access_Category', 
                'wifi6', 
                'wlan.bssid', 
                'frame.time', 
                'WirelessEpoch']

avg_latency_cols = ['Avg Latency(us)', 
                    'Number of Frames', 
                    'Frame.Time',
                    'Platform']

type_subtype_dictionary ={0 : "AssocReq", 
                          1 : "AssocResp", 
                          2 : "ReAssocReq", 
                          3: "ReAssocResp",
                          4: "ProbeReq", 
                          5: 'ProbeResp',
                          8: 'Beacon', 
                          9: 'ATIM', 
                          10: 'Disassoc', 
                          11: 'Auth',
                          12: 'DeAuth', 
                          13: 'Action',
                          0x18: 'BAR', 
                          0x19:'BA', 
                          0x1a:'PS-Poll', 
                          0x1b:'RTS', 
                          0x1c:'CTS',
                          0x1d:'ACK', 
                          0x1e:'CF-end', 
                          0x1f:'Reserved1f', 
                          0x20:'Data', 
                          0x21:"Data+CF-ack",
                          0x22:'Data + CF-poll',
                          0x23:"Data +CF-ack +CF-poll",
                          0x24:"Null", 
                          0x25:'CF-ack', 
                          0x26:'CF-poll', 
                          0x27:'CF-ack +CF-poll',
                          0x28:'QoS data',
                          0x29:'QoS data + CF-ack',
                          0x2a:'QoS data + CF-poll',
                          0x2b:'QoS data + CF-ack + CF-poll',
                          0x2c:'QoS Null', 
                          0x15:'VHT-Sounding'}
# Create the dictionary
type_dictionary ={0. : "Mgmt", 
                  2. : "Data", 
                  1. : "Control", 
                  3.: "Extension"}


phytype_type_dictionary = { 1:"802.11 FHSS",
                            2: "802.11 IR",
                            3: "802.11 DSSS",
                            4: "802.11b (HR/DSSS)",
                            5: "802.11a (OFDM)",
                            6: "802.11g (ERP)",
                            7: "802.11n (HT)",
                            8: "802.11ac (VHT)",
                            9: "802.11ad (DMG)",
                            10: "802.11ah (S1G)",
                            11: "802.11ax (HE)"}

access_category_dict ={0. : "BE", 
                       3. : "BE", 
                       2. : "BK", 
                       1. : "BK",
                       4. : "VI", 
                       5. : 'VI', 
                       6. : 'VO', 
                       7. : 'VO'}

# 11ax data rate cacluation.
# number of cariers dependending on channel width for 20, 40, 80, 160
sub_carrier_dict = {'0x00000000':234,
                    '0x00000001':468,
                    '0x00000002':980,
                    '0x00000003':1960}

# number of coded bits for signal
coded_bits_dict = {'0x00000000':1/2,
                   '0x00000001':2*1/2,
                   '0x00000002':2*3/4,
                   '0x00000003':4*1/2,
                   '0x00000004':4*3/4,
                   '0x00000005':6*2/3,
                   '0x00000006':6*3/4,
                   '0x00000007':6*5/6,
                   '0x00000008':8*3/4,
                   '0x00000009':8*5/6,
                   '0x0000000a':10*3/4,
                   '0x0000000b':10*5/6}

# MCS 
mcs_dict =        {'0x00000000':0,
                   '0x00000001':1,
                   '0x00000002':2,
                   '0x00000003':3,
                   '0x00000004':4,
                   '0x00000005':5,
                   '0x00000006':6,
                   '0x00000007':7,
                   '0x00000008':8,
                   '0x00000009':9,
                   '0x0000000a':10,
                   '0x0000000b':11}

# Spatial Streams
nss_dict =        {'0x00000001':1,
                   '0x00000002':2,
                   '0x00000003':3,
                   '0x00000004':4,
                   '0x00000005':5,
                   '0x00000006':6,
                   '0x00000007':7,
                   '0x00000008':8}

# Guard internal for 0.8, 1.6 and 3.2
gi_dict =         {'0x00000000':0.8,
                   '0x00000001':1.6,
                   '0x00000002':3.2}

# Symbol Duration for 11ax versus pre-11ax
symbol_duration_dict = {'ax':12.4,
                        'nax':3.2}


wd_fields = "-T fields -e frame.number -e frame.len -e frame.time -e frame.time_delta -e frame.time_epoch -e eth.type -e ip.id -e lanforge.seqno -e ixveriwave.sig_ts -E header=y -E separator=, -E quote=d"
ws_fields = "-T fields -e frame.number -e frame.len -e frame.time -e radiotap.ampdu.reference -e wlan.seq -e wlan.frag -e radiotap.he.data_3.data_mcs -e radiotap.he.data_5.gi -e radiotap.he.data_5.data_bw_ru_allocation -e radiotap.he.data_6.nsts -e ixveriwave.frame_length -e radiotap.length -e ixveriwave.l1info.rate -e frame.time_epoch -e wlan.qos.amsdupresent -e wlan_radio.phy -e wlan_radio.11n.mcs_index -e wlan_radio.11ac.mcs -e wlan_radio.data_rate -e wlan_radio.signal_dbm -e wlan.fc.type -e wlan.ra -e wlan.ta -e wlan.fc.type_subtype -e wlan.fc.retry -e wlan.qos.tid -e wlan.qos.priority -e wlan.fc.tods -e wlan.fc.fromds -e wlan.bssid -e wlan.staa -e wlan.da -e ixveriwave.mumask -e wlan.tag.number -e llc.type -e ip.id -e lanforge.seqno -e ixveriwave.sig_ts -E header=y -E separator=, -E quote=d"

aggregations_mu = {
        'frame.len':'sum',
        "wlan_radio.data_rate":'first',
        }
ppdu_cols = ["frame.number",
           "frame.len",
           "FrameType",           
           "FrameTypeSubType",
           "wlan_radio.data_rate",
           "RSSI",
           "wifi6",
           "wlan.seq",
           'wlan.staa',
           'wlan.frag',
           "MCS",
           "Access_Category",
           "wlan.fc.tods",
           "wlan.fc.fromds",
           "radiotap.ampdu.reference",
           "wlan.bssid",
           "wlan.da",
           "wlan.fc.retry",
           "AMSDU"]

mu_cols = ["frame.number",
           "frame.len",
           "FrameType",           
           "FrameTypeSubType",
           "wlan_radio.data_rate",
           "RSSI",
           "wifi6",
           "MCS",
           'wlan.frag',
           'wlan.staa',
           "Access_Category",
           "wlan.fc.tods",
           "wlan.fc.fromds",
           "wlan.bssid",
           "wlan.da",
           "wlan.seq",
           "wlan.fc.retry",
           "AMSDU",
           "ixveriwave.mumask"  ]

# Function to bucketizee RSSI
def selector_rssi(x):
    """Return a RSSI Bucket as Good, Average, Bad 
       Mandatory parameter RSSI value
    """
    rssi='A'
    if x < -80:
        rssi = 'B'
    elif x < -60:
        rssi = 'A'
    else:
        rssi = 'G'
    return rssi


# Dictionary  for returning RSSI Bucket 
signal_dictionary = {x: selector_rssi(x) for x in range(-100,0)}

def parse_print_show_output(result_file):
    """ Parse output show controller dot11 traffic-dis exported
    """
    show = []
    var = False
    
    with open(result_file) as fp:
        for cnt, line in enumerate(fp):

            if len(line) < 5:       # ignore line less than 3 chars
                var = False
                continue                    
            if (('Pkg' in line) or (var == True)):
                #print("Line {}: {} {}".format(cnt, len(line), line))
                x = [i for i in line.split()]
                show.append(x)  # append each linie to the show list..
                var = True
                #print(x)
    # create 4 dfs
    #print(show)
    package_1 = pd.DataFrame()
    package_2 = pd.DataFrame()
    package_3 = pd.DataFrame()
    package_4 = pd.DataFrame()
    pkg1 = ''
    pkg2 = ''
    pkg3 = ''
    pkg4 = ''
    bad_chars = [';', ':', '!', "\\"] 
    for elem in show: 
        for index, e in enumerate(elem):
            if '\\' in e:            
                #print(e, index, elem[index], elem[-1], e[:-1])
                for i in bad_chars:
                    str = e.replace(i, '')
                elem[index] = str
        if 'Pkg' in elem and '1:' in elem:
            pkg1 = ' '.join(elem)
            #print(pkg1)
            p = 'pkg1'

            continue
        if 'Pkg' in elem and '2:' in elem:
            pkg2 = ' '.join(elem)
            p = 'pkg2'        
            #print(pkg2)

            continue        
        if 'Pkg' in elem and '3:' in elem:
            pkg3 = ' '.join(elem)
            p = 'pkg3'        
            #print(pkg3)

            continue        
        if 'Pkg' in elem and '4:' in elem:
            pkg4 = ' '.join(elem)
            #print(pkg4)
            p = 'pkg4'        

            continue
        if p == "pkg2" or p == "pkg3":
            #print(elem)
            if "Best" in elem:
                elem.remove("Effort")
    
        if p == 'pkg1':
            if (package_1.columns.empty):
                package_1 = pd.DataFrame(columns = elem)
                    #print(current_pkg.columns)
            else:
                #print(elem)
                # Pass a series in append() to append a row in dataframe  
                package_1 = package_1.append(pd.Series(elem, index=package_1.columns), 
                                                 ignore_index=True)
                #print(current_pkg)
        if p == 'pkg2':
            if (package_2.columns.empty):
                package_2 = pd.DataFrame(columns = elem)
                #print(package_2.columns)
            else:
                #print(elem)
                # Pass a series in append() to append a row in dataframe  
                package_2 = package_2.append(pd.Series(elem, index=package_2.columns), 
                                                 ignore_index=True)
        if p == 'pkg3':
            if (package_3.columns.empty):
                package_3 = pd.DataFrame(columns = elem)
                #print(package_3.columns)
            else:
                #print(elem)
                if (len(elem) == package_3.shape[1]):
                    # Pass a series in append() to append a row in dataframe  
                    package_3 = package_3.append(pd.Series(elem, index=package_3.columns), 
                                                 ignore_index=True)
                else:
                    print("Error - elements and columns differ for package 3")
                    print(package_3.columns)                    
                    print(elem)
        if p == 'pkg4':
            if (package_4.columns.empty):
                package_4 = pd.DataFrame(columns = elem)
                print(package_4.columns)
            else:
                if (len(elem) == package_4.shape[1]):
                    # Pass a series in append() to append a row in dataframe  
                    package_4 = package_4.append(pd.Series(elem, index=package_4.columns), 
                                                 ignore_index=True)
                else:
                    print("Error - elements and columns differ for package 4")
                    print(package_4.columns)                    
                    print(elem)          
    print(pkg1)
    print(package_1)
    print(pkg2)
    print(package_2)
    print(pkg3)
    print(package_3)
    print(pkg4)
    print(package_4)
    return package_1, package_2, package_3, package_4

def find_and_return_msdu_group(df, g):
    """ finds wireless frame with same wlan.seq, wlan.frag for a client<->bssid
    """
    msdu_grp = g.groupby(["wlan.frag", "wlan.bssid", 'wlan.staa', 'wlan.seq', 'wlan.fc.retry'])
    for n, gr in msdu_grp:
        #print("... ", n)
        #print("... ", gr.shape)
        size_of_group = gr.shape[0]
        start = gr.index[0]
        end = gr.index[-1]
        split_pkt_count = [round(1/size_of_group,10) for _ in range(size_of_group)]        
        #print("****size_of_group", size_of_group, start, end, end-start, split_pkt_count)
        if (size_of_group == (end - start +1)):
            df['mpdu_pkt_count'].iloc[start:end+1] = split_pkt_count        
        else:
            print("Error msdu group has unexpected sequence **********")
            print("group size", size_of_group, "start" , start, "end", end )
    return df
    
def find_and_return_ppdu_group(df):
    # Search for all Data frames for a BSSID in between two BlockAcks
    newdf = df.copy()
    
    df = df[df['Tech'] == 'SU']
    df = df[(df['FrameType'] == 'Control') |
            ((df['FrameType'] == 'Data'))]
    if df[df['FrameType'] == 'Data'].empty:
        print("empty Data Frame")
        return newdf
    
    df=df[ppdu_cols]

    m = df.loc[:, 'FrameType'].eq("Control")
    cumgrp = m.cumsum()[~m]
    ppdu_group_count=0
    grps = df[~m].groupby(cumgrp)
    for n, g in grps:
        ppdu_group_count += 1
        size_of_group = g.shape[0]
        start = g.index[0]
        end = g.index[-1]               
        airtime = (plcp+sifs)
        newdf = find_and_return_msdu_group(newdf, g)
        split_airtime = [round(airtime/size_of_group,1) for _ in range(size_of_group)]
        split_pkt_count = [round(1/size_of_group,10) for _ in range(size_of_group)]        
        logging.debug("size_of_group %d %d %d %d %s %d",size_of_group, start, end, end-start, g.index, len(split_airtime))
       
        newdf['airtime in usec'].iloc[start:end+1,] += split_airtime
        newdf['pkt_count'].iloc[start:end+1,] = split_pkt_count 
        
        

    logging.debug("PPDU Airtime DF")
    #print(newdf.reset_index())
    newdf.to_csv(ws_file+"_file_ppdu.csv", sep=',', encoding='utf-8')

    logging.debug(newdf.head(10))
    return newdf

def find_and_return_mu_group(df):
    # Search for all Data frames for a BSSID in between two BlockAcks
    newdf = df.copy()
    df = df[~((df['FrameType'] == 'Data') & (df['ixveriwave.mumask'].isna()))] 
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning) 
        df = df[(df['FrameType'] == 'Control') | 
                ((df['FrameType'] == 'Data') &
                 (df['ixveriwave.mumask'] != '0x00000000'))]
    if df[df['FrameType'] == 'Data'].empty:
        #print("*****Emtpy",df[df['FrameType'] == 'Data'].shape)    
        return newdf
    df=df[mu_cols]
    logging.debug(df.shape)
    #print(df.head(50))
    m = df.loc[:, 'FrameType'].eq("Control")
    cumgrp = m.cumsum()[~m]
    mu_group_count=0
    grps = df[~m].groupby(cumgrp)
    for n, g in grps:
        #print(g)
        mu_group_count += 1        
        #g = g[((g['FrameTypeSubType'] == 'QoS data') | (g['FrameTypeSubType'] == 'QoS Null'))]
        size_of_group = g.shape[0]
        start = g.index[0]
        end = g.index[-1]        
        #print("MU Group", mu_group_count)
        #print(g)
        a = g.groupby('ixveriwave.mumask').agg(aggregations_mu)
        a['airtime'] = (a['frame.len'] * 8)/(a['wlan_radio.data_rate'])
        #print("MU Group aggregated by MU MASK for MU Group", mu_group_count)
        #print(a)        
        airtime = a['airtime'].max()+plcp+sifs

        split_airtime = [round(airtime/size_of_group,1) for _ in range(size_of_group)]
        split_pkt_count = [round(1/size_of_group,10) for _ in range(size_of_group)]        
        logging.debug("size_of_group %d %d %d %d %s %d",size_of_group, start, end, end-start, g.index, len(split_airtime))        
        newdf['airtime in usec'].iloc[start:end+1] = split_airtime
        newdf['pkt_count'].iloc[start:end+1] = split_pkt_count
        for ndex, b in g.groupby('ixveriwave.mumask'):
            #print(b)
            newdf = find_and_return_msdu_group(newdf, b)
        
    
    logging.debug("MU Airtime DF")
    #print(newdf.reset_index())
    newdf.to_csv(ws_file+"_file_mu.csv", sep=',', encoding='utf-8')

    logging.debug(newdf.head(10))
    return newdf


def wifi6_data_rate(df):
    nss = df["radiotap.he.data_6.nsts"].map(lambda x: nss_dict.get(x,0))
    subcarrier = df["radiotap.he.data_5.data_bw_ru_allocation"].map(lambda x: sub_carrier_dict.get(x, 0)) 
    coded_bits = df["radiotap.he.data_3.data_mcs"].map(lambda x: coded_bits_dict.get(x, 0)) 
    gi = df["radiotap.he.data_5.gi"].map(lambda x: gi_dict.get(x,0))

    logging.debug(nss.value_counts())
    logging.debug(subcarrier.value_counts())
    logging.debug(coded_bits.value_counts()) 
    logging.debug(gi.value_counts())     
    symbol_duration = symbol_duration_dict['ax']

    df["11axdata_rate"] = (nss * subcarrier * coded_bits)/(gi + symbol_duration)
    df["wlan_radio.data_rate"] = np.where((df['wlan_radio.phy'] == 11), 
                                          df["11axdata_rate"],
                                          df["wlan_radio.data_rate"])
    logging.debug(df["wlan_radio.data_rate"].value_counts())

def get_wd_csv(wd_pcap):
    wd_file=wd_pcap+".csv"
    
    wd_list = "tshark -r"+" "+wd_pcap+" "+wd_fields
    tshark_wd = wd_list.strip().split(' ') 
    logging.debug("Convert pcap to csv:%s to %s", wd_pcap, wd_file)
    logging.debug(tshark_wd)
    f = open(os.path.join("./",wd_file), "w")
    subprocess.Popen(tshark_wd, stdout = f).communicate()
    print(' '.join(tshark_wd))
    f.close()
    return wd_file

def get_ws_csv(ws_pcap):
    ws_list = "tshark -r"+" "+ws_pcap+" "+ws_fields
    ws_file=ws_pcap+".csv"
    tshark_ws = ws_list.strip().split(' ')
    logging.debug("Convert pcap to csv file:%s to %s", ws_pcap, ws_file)
    logging.debug(tshark_ws)    
    f = open(os.path.join("./",ws_file), "w")
    subprocess.Popen(tshark_ws, stdout = f).communicate()
    print(' '.join(tshark_ws))
    f.close()
    return ws_file

def get_wifi6_assoc(df_ws):
    wifi6_assoc = df_ws.loc[:, ['wlan.staa']][(df_ws['wlan.tag.number'].notnull()) &
                (df_ws['wlan.tag.number'].str.contains('255')) &
                (df_ws['FrameTypeSubType'] == 'AssocReq')]
    logging.info("WiFi6 Association Requests from all Stations in the capture")
    logging.info(wifi6_assoc)
    return wifi6_assoc


def get_nonwifi6_assoc(df_ws):
    nonwifi6_assoc = df_ws.loc[:,['wlan.staa']][((df_ws['wlan.tag.number'].notnull()) | (df_ws['wlan.tag.number'].str.contains('255')!=0)) &
                         (df_ws['FrameTypeSubType'] == 'AssocReq')]
    logging.info("Non-WiFi6 Association Requests from all Stations in the capture")
    logging.info(nonwifi6_assoc)
    return nonwifi6_assoc

def qos_data_dist(df_ws, var, direction):
    df_qos_data = df_ws[((df_ws.FrameTypeSubType =='QoS data') | 
                        (df_ws.FrameTypeSubType =='QoS Null')) &
                        ((df_ws[direction] == 1) | (df_ws[direction] == '1')) &
                        (('' in  bssid_string)  | 
                        (df_ws['wlan.bssid'].isin(bssid_string)))
                        ][var].value_counts(dropna=True).rename_axis(var).reset_index(name="Count", )
    logging.info("QoS Data Packet Distribution based on  %s %s", var, direction )
    logging.info(df_qos_data)
    #print(df_qos_data)
    return df_qos_data

def qos_data_distby2groups(df_ws, var1, var2):
    
    
    df_qos_data2grps = df_ws[['frame.number', 'FrameTypeSubType', var1, var2, 'wlan.bssid']][
                            (((df_ws.FrameTypeSubType =='QoS data') | 
                                    (df_ws.FrameTypeSubType =='QoS Null')) &
                            (('' in  bssid_string)  | (df_ws['wlan.bssid'].isin(bssid_string))) &
                            ((df_ws['wlan.fc.fromds'] == '1') | 
                                    (df_ws['wlan.fc.fromds'] == 1)))].copy().reset_index()
    
    df_qos_data2grps['COUNTER'] = 1
    logging.debug("Tx Distribution QoS data frames based on %s and %s", var1, var2)
    logging.debug(df_qos_data2grps.groupby([var1, var2])['COUNTER'].sum().sort_values(ascending=False).head(40))

    df_qos_data2grps['COUNTER'] = 1
    logging.debug("Tx Distribution QoS data frames based on %s",var1)
    logging.debug(df_qos_data2grps.groupby([var1])['COUNTER'].sum().sort_values(ascending=False).head(40))
    
    df_qos_data2grps = df_ws[['frame.number', 'FrameTypeSubType', var1, var2, 'wlan.bssid']][
                            (((df_ws.FrameTypeSubType =='QoS data') | 
                                    (df_ws.FrameTypeSubType =='QoS Null')) &
                            (('' in  bssid_string ) | (df_ws['wlan.bssid'].isin(bssid_string))) &
                            (df_ws['wlan.fc.retry'] == 1) &
                            ((df_ws['wlan.fc.fromds'] == '1') | 
                                    (df_ws['wlan.fc.fromds'] == 1)))].copy().reset_index()
    
    df_qos_data2grps['COUNTER'] = 1
    logging.debug("Tx Retry Distribution QoS data frames based on %s and %s", var1, var2)
    logging.debug(df_qos_data2grps.groupby([var1, var2])['COUNTER'].sum().sort_values(ascending=False).head(40))

    
    df_qos_data2grps = df_ws[['frame.number', 'FrameTypeSubType', var1, var2, 'wlan.bssid']][
                        (((df_ws.FrameTypeSubType =='QoS data') | 
                                (df_ws.FrameTypeSubType =='QoS Null')) &
                        (('' in  bssid_string)  | (df_ws['wlan.bssid'].isin(bssid_string))) &
                        ((df_ws['wlan.fc.tods'] == '1') | 
                                (df_ws['wlan.fc.tods'] == 1)))].copy().reset_index()
    df_qos_data2grps['COUNTER'] = 1
    logging.debug("Rx Distribution QoS data frames based on %s and %s", var1, var2)
    logging.debug(df_qos_data2grps.groupby([var1, var2])['COUNTER'].sum().sort_values(ascending=False).head(40))
        
    df_qos_data2grps['COUNTER'] = 1
    logging.debug("Rx Distribution QoS data frames based on %s", var1)
    logging.debug(df_qos_data2grps.groupby([var1])['COUNTER'].sum().sort_values(ascending=False).head(40))
    
    df_qos_data2grps = df_ws[['frame.number', 'FrameTypeSubType', var1, var2, 'wlan.bssid']][
                        (((df_ws.FrameTypeSubType =='QoS data') | 
                                (df_ws.FrameTypeSubType =='QoS Null')) &
                        (('' in  bssid_string ) | (df_ws['wlan.bssid'].isin(bssid_string))) &
                        (df_ws['wlan.fc.retry'] == 1) &                        
                        ((df_ws['wlan.fc.tods'] == '1') | 
                                (df_ws['wlan.fc.tods'] == 1)))].copy().reset_index()
    df_qos_data2grps['COUNTER'] = 1
    logging.debug("Rx Retry Distribution QoS data frames based on %s and %s", var1, var2)
    logging.debug(df_qos_data2grps.groupby([var1, var2])['COUNTER'].sum().sort_values(ascending=False).head(40))
    


def frame_type_dist(df_ws, var):
    df_frame_type = df_ws[var].value_counts(dropna=False).rename_axis('FrameType').reset_index(name="Count", )
    logging.info("Distribution based on FrameType %s", var )
    logging.info(df_frame_type)
    return df_frame_type

def get_ws_qdf(ws_file, latency):
    
    global is_candela, is_veriwave
    
    df_ws = pd.DataFrame()
    
    df_ws = pd.read_csv(ws_file, sep=",", header = 0, usecols = ws_cols,
                     dtype={"wlan.llc.type" : "object",
                            "lanforge.seqno": "object",
                            "frame.len" :"int",
                            "wlan.fc.tods" : "object",
                            "wlan.fc.fromds": "object",
                            "wlan.fc.type_subtype": "object",
                            "radiotap.length":"float32",
                            "wlan.tag.number": "object",
                            "radiotap.he.data_3.data_mcs":"object",
                            "radiotap.he.data_5.gi":"object",
                            "radiotap.he.data_5.data_bw_ru_allocation":"object",
                            "radiotap.he.data_6.nsts":"object",                            
                            "ixveriwave.sig_ts": "object"},
                            low_memory=False)
 
    is_candela = (~df_ws['lanforge.seqno'].isna()).sum()
    is_veriwave = (~df_ws['ixveriwave.sig_ts'].isna()).sum()

    df_ws['MCS'] = [0 for _ in range(df_ws.shape[0])]
    
    if is_candela:
        df_ws = df_ws.rename(columns={'lanforge.seqno': 'key'})
        logging.info("Is Candela Capture")
        df_ws['frame.len'] = df_ws['frame.len'].apply(pd.to_numeric) - df_ws['radiotap.length'].apply(pd.to_numeric)
        df_ws['MCS'] = np.where((df_ws['wlan_radio.phy'] < 6), 0,
                                 np.where((df_ws['wlan_radio.phy'].isna()), 0,
                                 np.where((df_ws['wlan_radio.phy'] == 7),
                                 df_ws['wlan_radio.11n.mcs_index'],
                                 np.where((df_ws['wlan_radio.phy'] == 8),
                                 df_ws['wlan_radio.11ac.mcs'],
                                 np.where((df_ws['wlan_radio.phy'] == 11),
                                 df_ws['radiotap.he.data_3.data_mcs'].map(lambda x: mcs_dict.get(x,0)),                                         
                                 0)))))       
        wifi6_data_rate(df_ws)
        df_ws = df_ws.drop(columns = ['radiotap.he.data_3.data_mcs', 
                              'radiotap.he.data_5.gi' , 
                              'radiotap.he.data_5.data_bw_ru_allocation', 
                              'radiotap.he.data_6.nsts',
                              'ixveriwave.sig_ts'])           
        

    if is_veriwave:
        df_ws = df_ws.rename(columns={'ixveriwave.sig_ts': 'key'})
        logging.info("Is VW Capture")
        df_ws['frame.len'] = np.where(df_ws['ixveriwave.frame_length'].apply(pd.to_numeric).isna(), 
             df_ws['frame.len'].apply(pd.to_numeric) - ixvw_hdr_len + fcs,  
             df_ws['ixveriwave.frame_length'].apply(pd.to_numeric))
        df_ws['frame.len'] = np.where(df_ws['frame.len'] <0, 0, df_ws['frame.len'])

        df_ws['MCS'] = np.where(((df_ws['wlan_radio.phy'] < 6) | (df_ws['wlan_radio.phy'].isna())), 0,
                          np.where((df_ws['wlan_radio.phy'] == 7), 
                                df_ws['wlan_radio.11n.mcs_index'],
                                df_ws['wlan_radio.11ac.mcs'])) 
        df_ws = df_ws.drop(columns = ['radiotap.he.data_3.data_mcs', 
                              'radiotap.he.data_5.gi' , 
                              'radiotap.he.data_5.data_bw_ru_allocation', 
                              'radiotap.he.data_6.nsts',
                              'lanforge.seqno'])        
    

    # Update the FrameSubType in English
    df_ws["wlan.fc.type_subtype"] = df_ws["wlan.fc.type_subtype"].str.split(',').str.get(0)
    df_ws = df_ws.astype({'wlan.fc.type_subtype': 'float32'})
    if (df_ws[df_ws['wlan.fc.type_subtype'] == 40].empty):
        logging.info("***Empty Data Frame***")
        return pd.DataFrame(), pd.DataFrame()
    
    df_ws['FrameTypeSubType'] = df_ws['wlan.fc.type_subtype'].map(lambda x: type_subtype_dictionary.get(x, "RSVD"))
    # drop the original column
    df_ws = df_ws.drop(columns = 'wlan.fc.type_subtype')
    
    #Get all Assocs Frame with WiFi6 Tag
    wifi6_assoc = get_wifi6_assoc(df_ws)
    
    #Get all Assocs Frame with WiFi6 Tag
    nonwifi6_assoc = get_nonwifi6_assoc(df_ws)
    
    #Create a dictionary of non wifi6 assocs
    nonwifi6_assoc_dictionary = {k:'nax' for k in list(nonwifi6_assoc['wlan.staa'])}
    
    #Create a dictionary of non wifi6 assocs
    wifi6_assoc_dictionary = {k:'ax' for k in list(wifi6_assoc['wlan.staa'])}
    
    # update the Column to WiFi6
    df_ws.loc[:, 'wifi6'] = df_ws.loc[:, 'wlan.staa'].map(lambda x: nonwifi6_assoc_dictionary.get(x, client))
    df_ws.loc[:, 'wifi6'] = df_ws.loc[:, 'wlan.staa'].map(lambda x: wifi6_assoc_dictionary.get(x, client))
    
    
    # update the Access Cateogry columns
    df_ws['Access_Category'] = df_ws['wlan.qos.priority'].map(lambda x: access_category_dict.get(x, "NaN"))
    
    df_ws =  df_ws.drop(columns = 'wlan.qos.priority')    
    
    #logging.debug(df_ws.info())
    #logging.debug(df_ws['wifi6'].unique())
    
    # create a fram for latency cal. Drop all rows whhich does not key or key is 0.
    drop_rows = df_ws_qdf = df_ws[((df_ws['key'].isna()) | (df_ws['key'] == '0'))].shape[0]
    df_ws_qdf = df_ws[~((df_ws['key'].isna()) | (df_ws['key'] == '0'))].copy()
    
    logging.debug("Dropped Rows %d", drop_rows)
    
    # 
    df_ws['AMSDU'] = df_ws['key'][(df_ws['FrameTypeSubType'] == 'QoS data') |
            (df_ws['FrameTypeSubType'] == 'QoS Null')].str.split(',').str.len()
    


    df_ws['AMSDU'] = np.where(df_ws['AMSDU'].isna(), amsdu_count, df_ws['AMSDU'])
    logging.debug("AMSDU Count Packet %d", df_ws['AMSDU'].sum())    
    

    for (columnName, columnData) in df_ws.iteritems():
        if df_ws[columnName].dtype == 'O':
            if ((columnName != 'key') & (columnName != 'MCS')):
                #print(columnName)
                df_ws[columnName] = df_ws[columnName].str.split(',').str.get(0)
    
    df_ws = df_ws.astype({'wlan.fc.type':'float32',
                          'wlan.fc.retry': 'float32',
                          'wlan.fc.fromds': 'object',
                          'ixveriwave.mumask': 'object',
                          'wlan.fc.tods': 'object',
                          'wlan.tag.number':'object',
                          'wlan_radio.data_rate': 'float32'}) 

    # Add a new column named 'FrameType'
    df_ws['FrameType'] = df_ws['wlan.fc.type'].map(lambda x: type_dictionary.get(x, None))
    df_ws['PhyType'] = df_ws['wlan_radio.phy'].map(lambda x: phytype_type_dictionary.get(x, "NaN"))
    
    # If signal is in the range of -100 to 0 then return based on function else default to average
    df_ws['RSSI'] = df_ws['wlan_radio.signal_dbm'].map(lambda x: signal_dictionary.get(x, "A"))
    df_ws['wlan_radio.data_rate'].fillna((df_ws['wlan_radio.data_rate'].mean()), inplace=True)
    df_ws = df_ws.drop(columns = ['wlan.fc.type', 
                                  'wlan_radio.signal_dbm', 
                                  'radiotap.length',
                                  'wlan_radio.11n.mcs_index',
                                  'wlan_radio.11ac.mcs',
                                  'wlan.qos.tid',
                                  'wlan.tag.number',
                                  'llc.type'])

    df_ws['airtime in usec'] = (df_ws['frame.len']*8)/df_ws['wlan_radio.data_rate']
    df_ws['Tech'] = ['SU' for _ in range(df_ws.shape[0])]
    
    if is_veriwave:
        with warnings.catch_warnings():
            warnings.simplefilter(action='ignore', category=FutureWarning) 
            df_ws['Tech'] = np.where((df_ws['ixveriwave.mumask'] == '0x00000000'), 'SU',
                            np.where(df_ws['ixveriwave.mumask'].isna(), 'SU', 'MU'))

    frame_type_dist(df_ws, 'FrameType')
    frame_type_dist(df_ws, 'FrameTypeSubType')
    for direction in ['wlan.fc.fromds', 'wlan.fc.tods']:
        qos_data_dist(df_ws, 'wlan_radio.data_rate', direction)
        qos_data_dist(df_ws, 'MCS', direction)
        qos_data_dist(df_ws, 'wlan.fc.retry', direction)
        qos_data_dist(df_ws, 'wifi6', direction)
        qos_data_dist(df_ws, 'RSSI', direction)
        qos_data_dist(df_ws, 'PhyType', direction)
        qos_data_dist(df_ws, 'AMSDU', direction)
        qos_data_dist(df_ws, 'wlan.staa', direction)
        qos_data_dist(df_ws, 'key', direction)
        qos_data_dist(df_ws, 'Access_Category', direction)
        qos_data_dist(df_ws, 'wlan.bssid',direction)
        qos_data_dist(df_ws, 'Tech',direction)        


    qos_data_distby2groups(df_ws, 'wlan_radio.data_rate', 'Tech')

    qos_data_distby2groups(df_ws, 'wlan.staa', 'Access_Category')   
    #print(df_ws)    
    df_ws = df_ws[((((df_ws['FrameTypeSubType'] == 'QoS data') | 
            (df_ws['FrameTypeSubType'] == 'QoS Null')) &
                 (('' in bssid_string) | (df_ws['wlan.bssid'].isin(bssid_string))) &
                  (df_ws['wlan.da'] != 'ff:ff:ff:ff:ff:ff'))
                  | (df_ws['FrameType'] == 'Control'))].reset_index()
    #print(df_ws)
    df_ws.fillna({'wlan.fc.retry':0, 'wlan.fc.fromds':0, 'wlan.fc.tods':0}, inplace=True)
    df_ws.to_csv(ws_file+"_file.csv", sep=',', encoding='utf-8')    
    df_ws = df_ws.astype({'wlan.fc.fromds':'int32',
                    'FrameTypeSubType':'object',
                    'wlan.fc.tods': 'int32',    
                    'AMSDU':'int32',
                    'wlan.fc.retry':'int32'})

    return df_ws_qdf,df_ws

def get_wd_df(wd_file):
    global is_candela, is_veriwave    
    df_wd = pd.read_csv(wd_file, sep=",", header = 0,
                        usecols = ["frame.number",
                                   "frame.len",
                                   "frame.time",
                                   "frame.time_delta",
                                   "frame.time_epoch",
                                   "eth.type",
                                   "ixveriwave.sig_ts",
                                   "lanforge.seqno"],
                                   dtype={
                                          "eth.type": "object",
                                          "ixveriwave.sig_ts":"object", 
                                          "lanforge.seqno": "object"},
                                          low_memory=False)
    
    # Filter out ip and ipv6 packets
    is_candela = (~df_wd['lanforge.seqno'].isna()).sum()
    is_veriwave = (~df_wd['ixveriwave.sig_ts'].isna()).sum()
    
    if is_candela:
        df_wd = df_wd.rename(columns={'lanforge.seqno': 'key'})
    
    if is_veriwave:
        df_wd = df_wd.rename(columns={'ixveriwave.sig_ts': 'key'})
        
    '''    
    df_wd['key'] = np.where(
            ((df_wd['ip.proto'] == '1') & ((df_wd['icmp.type'] == '8'))), 
            df_wd['icmp.seq']+df_wd['ip.checksum'],
            np.where((df_wd['ip.proto'] == '6'), 
                     df_wd['tcp.seq'] + df_wd['tcp.checksum'] + df_wd['tcp.ack'],
                     np.where((df_wd['ip.proto'] == '17'),
                              df_wd['udp.checksum'] +  df_wd['ip.checksum'],
                              20000)))
    '''
    
    df_wd = df_wd[~((df_wd['key'].isna()) | (df_wd['key'] == '0'))]
    new_df_wd = df_wd.reset_index()
            
    return new_df_wd

def update_main_airtime(df):
    file = pathlib.Path(airtime_file)
    logging.debug("Main Airtime")
    if file.exists():
        logging.debug("Main Airtime File exists")


        a_df = pd.read_csv(file, sep=",", header = 0,
                           index_col = 'Trtype',
                           low_memory=False)
        

        
        a_df = a_df.append(df)
        a_df = a_df.reset_index()
        a_df = a_df.groupby(['Trtype', 'Platform']).sum()
        a_df = a_df.reset_index()
        a_df = a_df.set_index('Trtype')        
        logging.debug(a_df)
        a_df.to_csv(file, sep=',', encoding='utf-8')
    else:
        logging.debug("Main Airtime File Does not exist")
        df.to_csv(file, sep=',', encoding='utf-8')
        logging.debug(df)        
    return

def update_main_latency(df):
    logging.debug("Main Latency")    
    file = pathlib.Path(latency_file)

    if file.exists():
        logging.debug("Main latency File exists")
        a_df = pd.read_csv(file, sep=",", header = 0,
                           index_col = 'Trtype',                           
                           low_memory=False)
        a_df = a_df.append(df)
        a_df.to_csv(file, sep=',', encoding='utf-8')
        logging.debug(a_df)
    else:
        logging.debug("Main Latency File Does not exist")        
        df.to_csv(file, sep=',', encoding='utf-8')
        logging.debug(df)
    return

def update_main_rate(df):
    file = pathlib.Path(rate_file)
    logging.debug("Main Rate")
    if file.exists():
        logging.debug("Main Rate File exists")
        a_df = pd.read_csv(file, sep=",", header = 0,
                           index_col = 'MCS',   
                           low_memory=False)
        a_df = a_df.append(df)
        a_df = a_df.reset_index()
        a_df = a_df.groupby(['MCS', 'Platform']).sum()
        a_df = a_df.reset_index()
        a_df = a_df.set_index('MCS')    
        logging.debug(a_df)
        a_df.to_csv(file, sep=',', encoding='utf-8')
    else:
        logging.debug("Main Rate File Does not exist")
        df.to_csv(file, sep=',', encoding='utf-8')
        logging.debug(df)        
    return

def get_data_pkt_bytes_count(df):

    df_r = df[
            ((df['FrameTypeSubType'] == 'QoS data') | (df['FrameTypeSubType'] == 'QoS Null')) &
            (('' in bssid_string) | (df['wlan.bssid'].isin(bssid_string))) &
            (df['wlan.da'] != 'ff:ff:ff:ff:ff:ff') 
             ].copy()

    df_r = df_r.astype({'wlan.fc.retry': 'float32',
                          'wlan.fc.fromds': 'float32',
                          'wlan.fc.tods': 'float32'})

    df_r['wlan.fc.tx_retry'] = (
                                ((df_r['wlan.fc.fromds'] == 1)) 
                                & (df_r['wlan.fc.retry'] == 1)
                                )
    df_r['wlan.fc.rx_retry'] = (
                                ((df_r['wlan.fc.tods'] == 1)) 
                                & (df_r['wlan.fc.retry'] == 1)
                                )     
                                
    df_r = df_r.astype({'wlan.fc.tx_retry':'int32',
                        'wlan.fc.rx_retry':'int32'})                   
    df_r_copy = df_r.copy()


    df_r_copy['T-Retry-Bytes'] = df_r_copy['wlan.fc.tx_retry'] * df_r_copy['frame.len']
    df_r_copy['T-Bytes'] = df_r_copy['wlan.fc.fromds'] * df_r_copy['frame.len'] - df_r_copy['T-Retry-Bytes']
    df_r_copy['T-airtime'] = df_r_copy['wlan.fc.fromds'] * df_r_copy['airtime in usec']

    df_r_copy['R-Retry-Bytes'] = df_r_copy['wlan.fc.rx_retry'] * df_r_copy['frame.len']    
    df_r_copy['R-airtime'] = df_r_copy['wlan.fc.tods'] * df_r_copy['airtime in usec']
    df_r_copy['R-Bytes'] = df_r_copy['wlan.fc.tods'] * df_r_copy['frame.len'] - df_r_copy['R-Retry-Bytes']
    

    df_r_copy['T-MSDU'] = df_r_copy['wlan.fc.fromds'] * df_r_copy['AMSDU']
    df_r_copy['R-MSDU'] = df_r_copy['wlan.fc.tods'] * df_r_copy['AMSDU']
    df_r_copy['T-Retry-MSDU'] = df_r_copy['wlan.fc.tx_retry'] * df_r_copy['AMSDU']
    df_r_copy['T-MSDU'] = df_r_copy['T-MSDU'] - df_r_copy['T-Retry-MSDU']
    
    df_r_copy['T-PPDU'] = df_r_copy['wlan.fc.fromds'] * df_r_copy['pkt_count'] 
    df_r_copy['R-PPDU'] = df_r_copy['wlan.fc.tods'] * df_r_copy['pkt_count'] 

    df_r_copy['T-MPDU'] = df_r_copy['wlan.fc.fromds'] * df_r_copy['mpdu_pkt_count'] 
    df_r_copy['R-MPDU'] = df_r_copy['wlan.fc.tods'] * df_r_copy['mpdu_pkt_count'] 
    df_r_copy['T-Retry-MPDU'] = df_r_copy['wlan.fc.tx_retry'] * df_r_copy['mpdu_pkt_count'] 
    df_r_copy['T-MPDU'] = df_r_copy['T-MPDU'] - df_r_copy['T-Retry-MPDU']
    
    df_r_copy['T-b/ms'] = (df_r_copy['T-Bytes'] * 8)/(1000 * df_r_copy['T-airtime'])
    df_r_copy['R-b/ms'] = (df_r_copy['R-Bytes'] * 8)/(1000 * df_r_copy['R-airtime'])
    df_r_copy.to_csv(ws_file+'_df_pkt_byte.csv', sep=',', encoding='utf-8')    
    return df_r_copy

def get_count(df, ds, ctrl_pkt):
    cols = ['wlan.ta', 'wlan.ra', 'wlan.fc.fromds', 'wlan.fc.tods', 'FrameTypeSubType', 'airtime in usec']
    pkt_count = 0
    bssid1 = 'wlan.ta'
    bssid2 = 'wlan.ra'
    if ds == 'wlan.fc.tods':
        bssid1 = 'wlan.ra'
        bssid2 = 'wlan.ta'

    if ctrl_pkt == 'rts':
        pkt = df[cols][(df.FrameTypeSubType == 'RTS') & (df[bssid1].isin(bssid_string))].reset_index()
        logging.debug(pkt.head(10))
    elif ctrl_pkt == 'cts':
        pkt = df[cols][(df.FrameTypeSubType == 'CTS') & (df[bssid2].isin(bssid_string))].reset_index()
        logging.debug(pkt.head(10))
    elif ctrl_pkt == 'ack':
        pkt = df[cols][(df.FrameTypeSubType == 'ACK') & (df[bssid2].isin(bssid_string))].reset_index()        
        logging.debug(pkt.head(10))       
    elif ctrl_pkt == 'back':
        pkt = df[cols][(df.FrameTypeSubType == 'BA') & (df[bssid2].isin(bssid_string))].reset_index()        
        logging.debug(pkt.head(10))
    elif ctrl_pkt == 'sounding':
        pkt = df[cols][(df.FrameTypeSubType == 'VHT-Sounding') & (df[bssid1].isin(bssid_string))].reset_index()        
        logging.debug(pkt.head(10))
    elif ctrl_pkt == 'back_req':
        pkt = df[cols][(df.FrameTypeSubType == 'BAR') & (df[bssid1].isin(bssid_string))].reset_index()        
        logging.debug(pkt.head(10))          
    pkt_count = pkt.shape[0]        
    pkt_airtime = pkt['airtime in usec'].sum()
    logging.debug("%s, %s, %d, %d",ctrl_pkt, ds, pkt_count, pkt_airtime)
    return pkt_count, pkt_airtime

def adjust_airtime_cal(df, airtime, dpkt):
    logging.debug("airtimetx %s direction %s", airtime, dpkt)
    airtime_list = []
    sum_tpkt= df[dpkt+'-PPDU'].sum()
    for pkt in df[dpkt+'-PPDU']:
        logging.debug("pkt %s sum of pkt %s", pkt, sum_tpkt)
        if sum_tpkt != 0:
            airtime_list.append(round(pkt*airtime/sum_tpkt, 0))
        else:
            airtime_list.append(0)

    df[dpkt+'-airtime'] += airtime_list
    df[dpkt+'adjust'] = airtime_list
    return df


def get_control_df(df_ws):
    control_df = pd.DataFrame(columns = ['RTS', 'CTS', 'ACK', 'BACK', 'BACK-REQ', 'VHT-Sounding'])    
     #df_ws.to_csv("rate_airtime.csv", sep=',', encoding='utf-8') 
    rts_tx,rts_tx_at  = get_count(df_ws, 'wlan.fc.fromds', 'rts')
    cts_tx, cts_tx_at = get_count(df_ws, 'wlan.fc.fromds', 'cts')
    ack_tx, ack_tx_at = get_count(df_ws, 'wlan.fc.fromds', 'ack')
    back_tx,back_tx_at  = get_count(df_ws, 'wlan.fc.fromds', 'back')
    back_req_tx,back_req_tx_at  = get_count(df_ws, 'wlan.fc.fromds', 'back_req')
    sounding_tx,sounding_tx_at  = get_count(df_ws, 'wlan.fc.fromds', 'sounding')    
    #airtime_tx = rts_tx_at + cts_tx_at + ack_tx_at + back_tx_at + back_req_tx_at+ sounding_tx_at

    rts_rx,rts_rx_at  = get_count(df_ws, 'wlan.fc.tods', 'rts')
    cts_rx, cts_rx_at = get_count(df_ws, 'wlan.fc.tods', 'cts')
    ack_rx, ack_rx_at = get_count(df_ws, 'wlan.fc.tods', 'ack')
    back_rx, back_rx_at = get_count(df_ws, 'wlan.fc.tods', 'back')
    back_req_rx,back_req_rx_at  = get_count(df_ws, 'wlan.fc.tods', 'back_req')
    sounding_rx,sounding_rx_at  = get_count(df_ws, 'wlan.fc.tods', 'sounding')    
    #airtime_rx = rts_rx_at + cts_rx_at + ack_rx_at + back_rx_at + back_req_rx_at + sounding_rx_at
    
    control_df.loc['Tx Count'] = [rts_tx, cts_tx, ack_tx, back_tx,back_req_tx,sounding_tx]
    control_df.loc['Rx Count'] = [rts_rx, cts_rx, ack_rx, back_rx, back_req_rx,sounding_rx ]
    control_df.loc['Tx AirTime'] = [rts_tx_at, cts_tx_at, ack_tx_at, back_tx_at, back_req_tx_at,sounding_tx_at]
    control_df.loc['Rx AirTime'] = [rts_rx_at, cts_rx_at, ack_rx_at, back_rx_at, back_req_rx_at, sounding_rx_at]
    print("Control DF")
    print(control_df.astype(int))
    logging.warning("Control DF")
    logging.warning(control_df.astype(int))
    return control_df
    
def traffic_distribution_by_mcs(df_rate_final):

    aggregations = {
    'T-Bytes'           : 'sum',
    'T-Retry-Bytes'     : 'sum',
    'T-airtime'         : 'sum',
    'R-Bytes'           : 'sum',
    'R-Retry-Bytes'     : 'sum',
    'R-airtime'         : 'sum',
    'T-MSDU'            : 'sum',
    'T-MPDU'            : 'sum',
    'T-PPDU'            : 'sum',    
    'R-MSDU'            : 'sum',
    'R-MPDU'            : 'sum',
    'R-PPDU'            : 'sum',    
    'T-Retry-MSDU'      : 'sum',
    'T-Retry-MPDU'      : 'sum'
    }
    df_rate_final = df_rate_final.groupby(['MCS']).agg(aggregations).reset_index()
    
    valid_mcs = ['0.0', '1.0', '2.0','3.0','4.0','5.0','6.0','7.0', '8.0','9.0','10.0','11.0','12.0', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    df_rate_final = df_rate_final[df_rate_final['MCS'].isin(valid_mcs)]
    df_rate_final = df_rate_final.set_index('MCS')
    df_rate_final = df_rate_final.astype({
    'T-Bytes'           : 'int64',
    'T-Retry-Bytes'     : 'int64',
    'T-airtime'         : 'int64',
    'R-Bytes'           : 'int64',
    'R-Retry-Bytes'     : 'int64',
    'R-airtime'         : 'int64',
    'T-MSDU'            : 'int32',
    'T-MPDU'            : 'int32',
    'T-PPDU'            : 'int32',    
    'R-MSDU'            : 'int32',
    'R-MPDU'            : 'int32',
    'R-PPDU'            : 'int32',
    'T-Retry-MSDU'      : 'int32',    
    'T-Retry-MPDU'      : 'int32'
    })
    df_rate_final["Platform"] = [ plat for _ in range(df_rate_final.shape[0])]
    df_rate_final.to_csv(ws_file+'_df_rate.csv', sep=',', encoding='utf-8')
    print("Rate versus MCS:")    
    print(df_rate_final)
    logging.warning("Rate versus MCS:")
    logging.warning(df_rate_final)
    update_main_rate(df_rate_final)

def distribution_by_property(df, tech, rssi, ax, ac):

    df = df[(df['RSSI'] == rssi) & 
            (df['Tech'] == tech) &
            (df['wifi6'] == ax) &            
            (df['Access_Category'] == ac)].copy()    
    return df
    
    
def airtime_cal(df_ws, bssid_string, ws_file):


    df_ws['pkt_count'] = [1 for _ in range(df_ws.shape[0])]
    df_ws['mpdu_pkt_count'] = [1 for _ in range(df_ws.shape[0])]    
    control_df = get_control_df(df_ws)
    ctrl_airtime_tx = control_df.loc['Tx AirTime'].sum()
    ctrl_airtime_rx = control_df.loc['Rx AirTime'].sum()
    
    df_ws = find_and_return_ppdu_group(df_ws)
    df_ws = find_and_return_mu_group(df_ws)
    df_ws = get_data_pkt_bytes_count(df_ws)    
    traffic_distribution_by_mcs(df_ws)
    astats = pd.DataFrame(columns=airtime_cols)    
    for tech in ['SU', 'MU']:
        for rssi in ['G', 'A', 'B']:
            for ax in ['nax', 'ax']:
                for ac in ['VO', 'VI', 'BE', 'BK']:
                    df_property = distribution_by_property(df_ws, tech, rssi, ax, ac)
                    index=ax+'-'+rssi+'-'+ac+'-'+tech
                    if ((df_property['T-airtime'].sum() != 0) | (df_property['R-airtime'].sum() != 0)):
                        astats.loc[index] = df_property[airtime_cols].sum()
        
    astats = adjust_airtime_cal(astats, ctrl_airtime_tx, 'T')
    astats = adjust_airtime_cal(astats, ctrl_airtime_rx, 'R')

    astats = astats.astype(int)
    astats["Platform"] = [plat for _ in range(astats.shape[0])]
    astats.index.name= 'Trtype'

    print("AirTime Stats:")
    print(astats)    
    logging.warning("AirTime Stats:")
    logging.warning(astats)
    astats.to_csv(ws_file+"_airtime.csv", sep=',' , encoding='utf-8')
    update_main_airtime(astats)

   
    

def print_latency(df, ac, ax):
    df_ac = df[((''  in bssid_string) | (df['wlan.bssid'].isin(bssid_string))) &
                 (df['wifi6'] == ax) &
                 (df['Access_Category'] == ac)].copy().reset_index()      
    if df_ac.empty:
        return np.nan, np.nan, np.nan, np.nan

    logging.debug("Frames with %s and %s", ac, ax)
    logging.debug(df_ac)
    latency = round(df_ac['latency in us'].mean(),3)
    num_pkt = df_ac.shape[0]

    ts = df_ac.at[0, 'frame.time']

    return (latency, num_pkt, ts, plat)

def latency_cal(df_ws, new_df_wd, ws_file):
    latency = pd.DataFrame(columns=avg_latency_cols)
    if (df_ws['key'].str.contains(',').sum()!=0):
        #Split rows with more than msdu
        df_ws_latency = pd.DataFrame(df_ws["key"].str.split(',').tolist(), 
                                 index=[df_ws['frame.time'], 
                                        df_ws['frame.number'],
                                        df_ws['frame.time_epoch'],
                                        df_ws['wlan.bssid'],
                                        df_ws['wifi6'],
                                        df_ws['Access_Category'],
                                        df_ws['ixveriwave.l1info.rate'],
                                        df_ws['wlan_radio.data_rate']]).stack()
        
        df_ws_latency = df_ws_latency.reset_index([0, 
                                                   'frame.time', 
                                                   'frame.number',
                                                   'frame.time_epoch',
                                                   'wlan.bssid',
                                                   'wifi6',
                                                   'Access_Category',
                                                   'ixveriwave.l1info.rate', 
                                                   'wlan_radio.data_rate'], name='key')
    else:
        df_ws_latency = df_ws[['frame.time',
                               'frame.number',
                               'frame.time_epoch',
                               'wlan.bssid',
                               'wifi6',
                               'Access_Category',
                               'ixveriwave.l1info.rate',
                               'wlan_radio.data_rate',
                               'key']].copy()
    
    # remove all duplicate rows based on one column except the last row
    df_ws_latency_new = df_ws_latency[~df_ws_latency.duplicated(subset=['key'], keep='last')]
    
    df_ws_latency_drop = df_ws_latency[df_ws_latency.duplicated(subset=['key'], keep='last')]

    new_df_wd = new_df_wd[['frame.number', 'frame.len', 'frame.time_epoch', 'frame.time_delta', 'key']]
    new_df_wd = new_df_wd.rename(columns={'frame.number': 'WiredFrameNo', 'frame.time_epoch':'WiredEpoch', 'frame.time_delta':'WiredDelta'})
    
    df_ws_latency_new = df_ws_latency_new.rename(columns={'frame.number': 'WirelessFrameNo', 'frame.time_epoch':'WirelessEpoch', 'frame.time_delta':'WirelessDelta'})
    logging.info("Left and Right DFs")
    logging.info(new_df_wd)
    logging.info(df_ws_latency_new)
    
    
    merge_df = pd.merge(left=new_df_wd, right=df_ws_latency_new, on='key', sort=True)
     # remove all duplicate rows based on one column except the last row
    merge_df = merge_df[~merge_df.duplicated(subset=['key'], keep='first')].reset_index().copy()
   
    merge_df['latency in us'] = (merge_df['WirelessEpoch'] - merge_df['WiredEpoch'])*1000*1000
    #merge_df = merge_df.sort_values(ascending=False, by=['latency inus'])
    merge_df = merge_df.sort_values(ascending=False, by=['latency in us'])
    logging.info("Final Latency CSV sorted in Latency in us")
    logging.info(merge_df.head(10))
    merge_df.to_csv(ws_file+"_latency.csv", sep=',', encoding='utf-8', index=False)
    
    for ac in ['BE', 'VI', 'VO', 'BK']:
        for ax in ['nax', 'ax']:
            index = ax+'-'+ac
            latency.loc[index] = print_latency(merge_df, ac, ax)
    
    latency = latency.dropna(subset=['Avg Latency(us)'])
    latency.index.name = 'Trtype'
    latency.to_csv(ws_file+"_Avglatency.csv", sep=',', encoding='utf-8')
    print("Avg Latency:")    
    print(latency)
    logging.warning("Avg Latency:")
    logging.warning(latency)
    logging.info("Avg Inter Packet Gap is %s us", round(new_df_wd['WiredDelta'].mean()*1000*1000,2))
    print_string = "Wired Side Frames: %s"
    logging.info(print_string, new_df_wd['WiredFrameNo'].count())
    print_string = "Total Wireless side frames after separating MPDUs  %s"
    logging.info(print_string, df_ws_latency['frame.number'].count())
    print_string = "Wireess Side Frames after subtracting retransmits from MPDUs  %s"
    logging.info(print_string, df_ws_latency_new['WirelessFrameNo'].count())
    print_string = "Wireess Side Frames with repeating key (seq number or timestamp) which was subtracted from total frames  %s"
    logging.info(print_string, df_ws_latency_drop['frame.number'].count())
    
    update_main_latency(latency)
        
    '''
    y = merge_df['latency in us']
    x = merge_df['WiredFrameNo']
    c = merge_df['ixveriwave.l1info.rate']
    plt.scatter(x, y, alpha = 0.8, c = merge_df["wlan_radio.data_rate"])
    plt.ylabel("Latency in us")
    plt.xlabel("Frame Number");
    plt.show()
    plt.xlabel("Latency in us")
    plt.hist(y)
    plt.show()
    
    
    plt.xlabel("Latency in us")
    plt.ylabel("wlan.fc.data_rate");
    plt.xlabel("Latency in us")
    plt.show()
    '''
    if 0:
        sns.scatterplot(y='latency in us', x="wlan_radio.data_rate",
                         style = 'wlan.fc.retry', 
                         size ='wlan_radio.data_rate', data=merge_df)
        plt.show()
            
# Read INI file 

config = configparser.ConfigParser()

# Read INI file 
section = config.read("sb.ini")

parser = argparse.ArgumentParser(description='Stormbreaker script')
parser.add_argument("-c", default='ax', type=str, help="Select WIFI6 Cleint (default AX)",
                    choices=['ax', "nax"],)

parser.add_argument("-p", default = 'BCM',
                    choices=["BCM", "QCA", "MVL", "QCAW2"],
                    required=False, type=str, 
                    help="Select Platform (default BCM)")

parser.add_argument("-d", default = 'DEBUG',
                    choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                    required=False, type=str, 
                    help="""Select Debug Level (default DEBUG)""")

parser.add_argument("-subdir", default = '',
                    required=False, type=str, 
                    help="Below Packets Directory if you want select subdirectory")

parser.add_argument("-mimo", default = 'n',
                    choices=['y', 'n'],                    
                    required=False, type=str, 
                    help="MU captures")

args = parser.parse_args()
client = args.c
plat = args.p
loglevel = args.d
subdir=args.subdir
mumimo=args.mimo

airtime = int(config[plat]['AIRTIME'])
pkt_dir=config['DEFAULT']['PACKET_ROOT_DIR']



directory = os.getcwd()+'/'+pkt_dir

if subdir:
    directory=directory+'/'+subdir

numeric_level = getattr(logging, loglevel.upper(), None)
if not isinstance(numeric_level, int):
    raise ValueError('Invalid log level: %s' % loglevel)
logging.basicConfig(
        level=numeric_level, format='%(message)s',
        filemode="w",
        filename='stormbreaker.log',
        )


print("start")
num_files = 0
list_of_files = []
final_main_lateency_df = pd.DataFrame(columns=[avg_latency_cols])
ws_file = ""
wd_file = ""
result_file = ''
text = ''
for root, dirs, files in os.walk(directory):
    ws_file = ""
    wd_file = ""
    result_file = ''

    for file in files:

        text = ''
        if file.endswith('.pcapng') or file.endswith('.vwr') or file.endswith('.pcap') or 'result' in file:
            num_files += 1
            list_of_files.append(file)
            text = ''
            #logging.critical("Procssing File %s", file)
            print("Procssing file", root+"/"+file)
            if 'wireless' in file or 'ch14' in file or 'moni' in file:
                ws_file = root+'/'+file

                logging.warning("Wireless file: %s", ws_file)

            if 'wired' in file or 'card1_port1' in file or 'eth' in file:
                wd_file = root+'/'+file
                logging.warning("Wired file: %s", wd_file)       
            if 'result' in file:
                result_file = root+'/'+file
                package_1, package_2, package_3, package_4 = parse_print_show_output(result_file)
                continue
            
            if 'barbados' in root+'/'+file:
                plat = 'MVL'
                client = 'nax'
            elif 'axel' in root+'/'+file:
                plat = 'QCA'
            elif 'vancouver' in root+'/'+file:
                plat = 'BCM'
            elif 'corsica' in root+'/'+file:
                plat = 'QCAW2'
                client = 'nax'
                
            if (mumimo =='y') or ('MU' in root+'/'+file):
                mumimo='y'
        
                
            if not ws_file and not wd_file and not result_file:
                logging.warning("please rename file: %s", file, "to have wireless or wired")
    

            bssid_string = config[plat]['BSSID'].split(',')
            amsdu_count = int(config['DEFAULT']['AMSDU_IGNORE'])
            plcp = int(config[plat]['PLCP'])
            sifs = int(config[plat]['SIFS'])
            #print(plat, bssid_string, ack, rts, back, cts, plcp, sifs)    
    
    if wd_file != '':
        df_wd_csv = get_wd_csv(wd_file)
        df_wd = get_wd_df(df_wd_csv)
        logging.debug("Wired Date Frames")
        logging.debug(df_wd.head(10))
        logging.debug(df_wd.dtypes)
            
    if ws_file != '':
        df_ws_csv = get_ws_csv(ws_file)
        df_ws, df_ws_mgt = get_ws_qdf(df_ws_csv, 1)
        logging.info("Platform Characertistics plat: %s bssid: %s, pclp: %d sifs: %d", plat, bssid_string, plcp, sifs)
        print("Platform Characertistics", "plat:", plat, "bssid:", bssid_string, "plcp:", plcp, "sifs:", sifs)        
        logging.debug("Wireless Data Frame")
        logging.debug(df_ws.head(10))
        logging.debug(df_ws.dtypes)


    if ((ws_file !='') and (wd_file !='') and (df_ws.empty != True)):
        df_ws_tx = df_ws[((df_ws['wlan.fc.fromds'] == '1') | (df_ws['wlan.fc.fromds'] == 1))]
        print("Latency Calculation")        
        latency_cal(df_ws_tx, df_wd, ws_file)

    if (ws_file != '' and  (df_ws.empty != True)):
        airtime_cal(df_ws_mgt, bssid_string, ws_file)
       
print("\n\nAggregations")
print("++++++++++++++++")

file = pathlib.Path(latency_file)

if file.exists():
    l_df = pd.read_csv(file, sep=",", header = 0, index_col = 'Trtype',
       low_memory=False)
    l_df = l_df.dropna(subset=['Avg Latency(us)'])
    avg = (l_df['Avg Latency(us)'] * l_df['Number of Frames']).sum()/l_df['Number of Frames'].sum()
    print("Final Latency frames:")
    logging.info("Final Latency frames:")
    print(avg)
    print(l_df)
    logging.info(avg)

file = pathlib.Path(airtime_file)

if file.exists():
    a_df = pd.read_csv(file, sep=",", header = 0,
                           usecols = airtime_cols+['Trtype', 'Platform'],
                           index_col = 'Trtype',
                           low_memory=False)
    #a_df = a_df.groupby(a_df.index).sum()
    print("Final AirTimeframe:")
    logging.info("Final AirTimeframe frames:")
    logging.info(a_df)
    print(a_df)

file = pathlib.Path(rate_file)
if file.exists():
    a_df = pd.read_csv(rate_file, sep=",", header = 0,
                       low_memory=False)

    print("Final Rate Frame:")
    logging.info("Final Rate frame frames:")
    logging.info(a_df.reset_index())
    print(a_df.reset_index())
logging.info("Files  Processed = %s", num_files)
logging.info(list_of_files)
print("Files  Processed = ", num_files)
print(list_of_files)

