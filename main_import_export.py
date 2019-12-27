# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 20:43:20 2019

@author: yazdsous
"""
#Libraries

import os
import datetime
import pandas as pd
from mailmerge import MailMerge
import mailmerge
import data_core_eforms as dce
import pyodbc

os.chdir(r'H:\GitHub\import-export-db')
os.getcwd()

conn0 = pyodbc.connect('Driver={SQL Server};'
                      'Server=Hosaka\Sqlp2;'
                      'Database=Eforms;'
                      'Trusted_Connection=yes;')

conn1 = pyodbc.connect('Driver={SQL Server};'
                      'Server=ndb-a1;'
                      'Database=Core;'
                      'Trusted_Connection=yes;')
#***********************************************************************
# GAS/OIL/NGL
# input: filingId (livelinkId)
# output: a list with extracted information from Core and Eforms 
#***********************************************************************
def order_data(filingid:str) -> list: 
    
    df_oas = dce.formfields_by_filingId(filingid, conn0)
    
    df_core = dce.rts_by_filingid(filingid, conn1)
    
    ctype = dce.application_type(df_oas[0])
    
    contacts = dce.contact_info(filingid, conn0) 
    
    ###Assumption: form before the board on the same day
    today = datetime.date.today()
    # dd/mm/YY
    today_date = today.strftime("%d %B %Y")
    #today_date_mc = today.strftime("%d %B %Y")
    current_year = today.year
    month = today.strftime("%d %B %Y").split()[1]
    before_the_board_en = r'XX ' + month + r' ' + str(current_year)
    before_the_board_fr = r'XX ' + dce.month_to_french(month) + r' ' + str(current_year)
    
    if df_core[1] == 0:
        return 'The filingid does not exist in RTS'
        exit
    ###########################################################################
    if ctype[0] == 'oil':
        ri = 'ROE-XXX-YYYY'
        start_end_order_date = list(dce.commence_end_order_oil(ctype, df_oas[0]))
    elif ctype[0] == 'ngl':
        ri = ['EPR-XXX-YYYY','EBU-XXX-YYYY']
        start_end_order_date = list(dce.commence_end_order_ngl(ctype, df_oas[0]))
    elif ctype[0] == 'gas':
#       ri = ['GO-XXX-YYYY','GO-XXX-YYYY']
        ri = 'GO-XXX-YYYY'
        start_end_order_date = list(dce.commence_end_order_gas(ctype, df_oas[0]))

    company = df_core[0].LegalName[0]
    file_ = df_core[0].FileNumber[0]
    before_the_board_date = [before_the_board_en,before_the_board_fr] 
   
    #Commodity type
    type_ = list(dce.comm_type_english_french(df_oas[0]))
   
    # Application date in french
    application_date_en = df_oas[0].AddedOn[0].strftime("%d %B %Y")
    une_demande_le_fr = dce.date_french(application_date_en)
    application_date = [application_date_en, une_demande_le_fr]
   
    ######Service standard
    enddate = dce.add_business_days(pd.to_datetime(application_date_en),2)
   
    #enddate = pd.to_datetime(today) + pd.DateOffset(days=2)
    Service_Standard  = enddate.strftime("%d/%m/%Y")

    list_values = [ctype, ri, company, file_, before_the_board_date, application_date, type_, start_end_order_date, filingid, Service_Standard]
    
    return list_values

###############################Populate forms for Natural Gas orders##############################
# Three templates are being used here:Export Only, Import Only, ExportAndImport                  #
##################################################################################################     
def populate_shortterm_app_form(filingid:str) -> str:  
    today_date = datetime.date.today().strftime("%d %B %Y")    
    info_for_mailmerge = order_data(filingid)     
    #GAS
    if info_for_mailmerge[0][0] == 'gas':
        #Form for Export ONLY AND Import ONLY orders
        if info_for_mailmerge[0][1] == 'gas_export_import':
            template = "Import_Export/tmp-final/727296 - TEMPLATE - Gas ExportImport Orders_p.docx"
            document = MailMerge(template)
            
            document.merge(
                  Application_Date = info_for_mailmerge[5][0],
                  Before__the_Bd_Date = info_for_mailmerge[4][1],
                 Company = info_for_mailmerge[2],
                 DEVANT___lOffice = info_for_mailmerge[4][1],
                 #Date_Sent_to_Walkaround = ,
                 #'EMail_Address',
                 Export_2 = info_for_mailmerge[1],
                 File_ = info_for_mailmerge[3],
                 Filing_ID = info_for_mailmerge[8],
                 GENRE = info_for_mailmerge[6][1],
                 Import_2 = info_for_mailmerge[1],
                 #'Name1',
                 #'Name2',
                 Order_Commences_ex = info_for_mailmerge[7][0],
                 Order_Ends_ex = info_for_mailmerge[7][1],
                 Order_Commences_im = info_for_mailmerge[7][2],
                 Order_Ends_im = info_for_mailmerge[7][3] ,
                 en_vigueur_le_ex =  info_for_mailmerge[7][4],
                 Ordre_se_termine_ex =  info_for_mailmerge[7][5],
                 en_vigueur_le_im =  info_for_mailmerge[7][6] ,
                 Ordre_se_termine_im =  info_for_mailmerge[7][7] ,
                 #'Salutation',
                 TYPE = info_for_mailmerge[6][0],
                 #'Title',
                 une_demande__le = info_for_mailmerge[5][1] )
            
            document.write(filingid+'_'+info_for_mailmerge[3]+'_'+today_date+'.docx')
            document.close()
            
        elif info_for_mailmerge[0][1] in ['gas_export','gas_import']:
            gtype = info_for_mailmerge[0][1]
            if gtype == 'gas_export':
                template = "Import_Export/tmp-final/727292 - TEMPLATE - Gas Export Order.docx"
            elif gtype == 'gas_import':
                template = "Import_Export/tmp-final/727294 - TEMPLATE - Gas Import Order.docx"
            
            document = MailMerge(template)
            
            document.merge(
                 Application_Date = info_for_mailmerge[5][0],
                 Before__the_Bd_Date = info_for_mailmerge[4][1],
                 Company = info_for_mailmerge[2],
                 DEVANT___lOffice = info_for_mailmerge[4][1],
                 #Date_Sent_to_Walkaround = ,
                 #'EMail_Address',
                 Export_1 = info_for_mailmerge[1],
                 Import_1 = info_for_mailmerge[1],
                 File_ = info_for_mailmerge[3],
                 Filing_ID = info_for_mailmerge[8],
                 GENRE = info_for_mailmerge[6][1],
                 #'Name1',
                 #'Name2',
                 Order_Commences = info_for_mailmerge[7][0] if gtype == 'gas_export' else info_for_mailmerge[7][2],
                 Order_Ends = info_for_mailmerge[7][1] if gtype == 'gas_export' else info_for_mailmerge[7][3],
                 en_vigueur_le =  info_for_mailmerge[7][4] if gtype == 'gas_export' else info_for_mailmerge[7][6],
                 Ordre_se_termine =  info_for_mailmerge[7][5] if gtype == 'gas_export' else info_for_mailmerge[7][7],
                 #'Salutation',
                 TYPE = info_for_mailmerge[6][0],
                 #'Title',
                 une_demande__le = info_for_mailmerge[5][1] )
            
            document.write(filingid+'_'+info_for_mailmerge[3]+'_'+today_date+'.docx')
            document.close()
    #NGL        
    elif info_for_mailmerge[0][0] == 'ngl':
        if info_for_mailmerge[0][1] == 'propane_butanes_export':
            template = "Import_Export/tmp-final/821263 - NGL NEW Orders Template ENGFR.docx"
            document = MailMerge(template)
            document.get_merge_fields()
            document.merge(
                 Application_Date = info_for_mailmerge[5][0],
                 Before__the_Bd_Date = info_for_mailmerge[4][0],
                 Company = info_for_mailmerge[2],
                 DEVANT___lOffice = info_for_mailmerge[4][1],
                 #Date_Sent_to_Walkaround = ,
                 #'EMail_Address',
                 Propane_Order = info_for_mailmerge[1][0],
                 File_ = info_for_mailmerge[3],
                 Filing_ID = info_for_mailmerge[8],
                 GENRE = info_for_mailmerge[6][1],
                 Butanes_Order = info_for_mailmerge[1][1],
                 #'Name1',
                 #'Name2',
                 Order_Commences_p = info_for_mailmerge[7][0],
                 Order_Ends_p = info_for_mailmerge[7][1],
                 Order_Commences_b = info_for_mailmerge[7][2],
                 Order_Ends_b = info_for_mailmerge[7][3] ,
                 en_vigueur_le_p =  info_for_mailmerge[7][4],
                 prend_fin_le_p =  info_for_mailmerge[7][5],
                 en_vigueur_le_b =  info_for_mailmerge[7][6] ,
                 prend_fin_le_b =  info_for_mailmerge[7][7] ,
                 #'Salutation',
                 TYPE = info_for_mailmerge[6][0],
                 #'Title',
                 une_demande__le = info_for_mailmerge[5][1] )
            
            document.write(filingid+'_'+info_for_mailmerge[3]+'_'+today_date+'.docx')
            document.close()
            
        elif info_for_mailmerge[0][1] in ['propane_export','butanes_export']:
            gtype = info_for_mailmerge[0][1]
            if gtype == 'propane_export':
                template = "Import_Export/tmp-final/821263 - NGL NEW Orders Template ENGFR_Propane_Only.docx"
            elif gtype == 'butanes_export':
                template = "Import_Export/tmp-final/821263 - NGL NEW Orders Template ENGFR_Butanes_Only.docx"
            
            document = MailMerge(template)
            
            document.merge(
                 Application_Date = info_for_mailmerge[5][0],
                 Before__the_Bd_Date = info_for_mailmerge[4][0],
                 Company = info_for_mailmerge[2],
                 DEVANT___lOffice = info_for_mailmerge[4][1],
                 #Date_Sent_to_Walkaround = ,
                 #'EMail_Address',
                 Propane_Order = info_for_mailmerge[1][0],
                 File_ = info_for_mailmerge[3],
                 Filing_ID = info_for_mailmerge[8],
                 GENRE = info_for_mailmerge[6][1],
                 Butanes_Order = info_for_mailmerge[1][1],
                 #'Name1',
                 #'Name2',
                 Order_Commences = info_for_mailmerge[7][0] if gtype == 'propane_export' else info_for_mailmerge[7][2],
                 Order_Ends = info_for_mailmerge[7][1] if gtype == 'propane_export' else info_for_mailmerge[7][3],
                 en_vigueur_le =  info_for_mailmerge[7][4] if gtype == 'propane_export' else info_for_mailmerge[7][6],
                 prend_fin_le =  info_for_mailmerge[7][5] if gtype == 'propane_export' else info_for_mailmerge[7][7],
                 #'Salutation',
                 TYPE = info_for_mailmerge[6][0],
                 #'Title',
                 une_demande__le = info_for_mailmerge[5][1] )
            
            document.write(filingid+'_'+info_for_mailmerge[3]+'_'+today_date+'.docx')
            document.close()
    #OIL      
    elif info_for_mailmerge[0][0] == 'oil':
        template = "Import_Export/tmp-final/847779 - Template  - New Applications - Crude Oil_.docx"
        document = MailMerge(template)
        
        document.merge(
            Application_Receipt_Date = info_for_mailmerge[5][0],
            Before_the_Commission = info_for_mailmerge[4][1],
            Company = info_for_mailmerge[2],
            Devant_lOffice = info_for_mailmerge[4][1],
            #Date_Sent_to_Walkaround = ,
            #'EMail_Address',
            ROE_ = info_for_mailmerge[1],
            File_ = info_for_mailmerge[3],
            Filing_ID = info_for_mailmerge[8],
            GENRE = info_for_mailmerge[6][1],
            #'Name1',
            #'Name2',
            Order_Commences = info_for_mailmerge[7][0],
            Order_Terminates = info_for_mailmerge[7][1],
            En_vigueur_le =  info_for_mailmerge[7][2],
            Prendra_fin_le =  info_for_mailmerge[7][3],
            #'Salutation',
            TYPE = info_for_mailmerge[6][0],
            #'Title',
            Une_demande_le = info_for_mailmerge[5][1] )
            
        document.write(filingid+'_'+info_for_mailmerge[3]+'_'+today_date+'.docx')
        document.close()
        

##########################################################################################







if __name__ == "__main__":
    
    populate_shortterm_app_form('C03102')

filingid = 'C03102'
today_date = datetime.date.today().strftime("%d %B %Y")
info_for_mailmerge = order_data(filingid)   

info_for_mailmerge = order_data(filingid)    
    #Form for Export ONLY or Import ONLY orders 

####TEST
order_data(filingid)
os.chdir(r'H:\GitHub\import-export-db')   
    
contacts = dce.contact_info('A98680', conn0) 
df_oas = dce.formfields_by_filingId(filingid, conn0)
df_oas[0].columns
df_core = dce.rts_by_filingid(filingid, conn1)
company = df_core[0].LegalName[0]
ctype = dce.application_type(df_oas[0])
order_data('C03367')
dce.comm_type_english_french(df_oas[0])    
####################CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC
os.getcwd()

template = r"H:\GitHub\import-export-db\Import_Export\tmp-final\821263 - NGL NEW Orders Template ENGFR.docx"
document = MailMerge(template)
document.get_merge_fields()
document.close()


dce.commence_end_order_oil(ctype,df_oas[0])

populate_shortterm_app_form(filingid)
template = "Import_Export/tmp-final/847779 - Template  - New Applications - Crude Oil_.docx"
document = MailMerge(template)
document.get_merge_fields()

filingid = 'C03492'
order_data(filingid)
populate_shortterm_app_form(filingid)