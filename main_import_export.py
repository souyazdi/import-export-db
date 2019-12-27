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
# GAS
# input: filingId (livelinkId)
# output: a list with extracted information from Core and Eforms 
#***********************************************************************

def gas_order_data(filingid:str) -> list: 
    
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
    comm_type = dce.application_type(df_oas[0])
    
    ri = 'GO-XXX-YYYY'

    company = df_core[0].LegalName[0]
    
    file_ = df_core[0].FileNumber[0]
    
    before_the_board_date = [before_the_board_en,before_the_board_fr] 
    
    # Application date in french
    application_date_en = df_oas[0].AddedOn[0].strftime("%d %B %Y")
    une_demande_le_fr = dce.date_french(application_date_en)
    application_date = [application_date_en, une_demande_le_fr]
    
    type_ = list(dce.comm_type_english_french(df_oas[0]))
    
    start_end_order_date = list(dce.commence_end_order_gas(ctype, df_oas[0]))
    
    ######Service standard
    enddate = dce.add_business_days(pd.to_datetime(application_date_en),2)
    #enddate = pd.to_datetime(today) + pd.DateOffset(days=2)
    Service_Standard  = enddate.strftime("%d/%m/%Y")
    

    list_values = [comm_type, ri, company, file_, before_the_board_date, application_date, type_, start_end_order_date, filingid, Service_Standard]
    
    return list_values

#***********************************************************************
# NGL
# input: filingId (livelinkId)
# output: a list with extracted information from Core and Eforms 
#***********************************************************************
def ngl_order_data(filingid:str) -> list: 
    
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
    comm_type = dce.application_type(df_oas[0])
    
    ri_b = 'EBU-XXX-YYYY'
    
    ri_p = 'EPR-XXX-YYYY'

    company = df_core[0].LegalName[0]
    
    file_ = df_core[0].FileNumber[0]
    
    before_the_board_date = [before_the_board_en,before_the_board_fr] 
    
    # Application date in french
    application_date_en = df_oas[0].AddedOn[0].strftime("%d %B %Y")
    une_demande_le_fr = dce.date_french(application_date_en)
    application_date = [application_date_en, une_demande_le_fr]
    
    #type_ = list(dce.comm_type_english_french(df_oas[0]))
    
    start_end_order_date = list(dce.commence_end_order_ngl(ctype, df_oas[0]))
    
    ######Service standard
    enddate = dce.add_business_days(pd.to_datetime(application_date_en),2)
    #enddate = pd.to_datetime(today) + pd.DateOffset(days=2)
    Service_Standard  = enddate.strftime("%d/%m/%Y")
    

    list_values = [comm_type, ri, company, file_, before_the_board_date, application_date, type_, start_end_order_date, filingid, Service_Standard]
    
    return list_values

#***********************************************************************
# OIL
# input: filingId (livelinkId)
# output: a list with extracted information from Core and Eforms 
#***********************************************************************
def oil_order_data(filingid:str) -> list: 
    
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
    comm_type = dce.application_type(df_oas[0])
    
    ri = 'ROE-XXX-YYYY'


    company = df_core[0].LegalName[0]
    
    file_ = df_core[0].FileNumber[0]
    
    before_the_board_date = [before_the_board_en,before_the_board_fr] 
    
    # Application date in french
    application_date_en = df_oas[0].AddedOn[0].strftime("%d %B %Y")
    une_demande_le_fr = dce.date_french(application_date_en)
    application_date = [application_date_en, une_demande_le_fr]
    
    #type_ = list(dce.comm_type_english_french(df_oas[0]))
    
    start_end_order_date = list(dce.commence_end_order_ngl(ctype, df_oas[0]))
    
    ######Service standard
    enddate = dce.add_business_days(pd.to_datetime(application_date_en),2)
    #enddate = pd.to_datetime(today) + pd.DateOffset(days=2)
    Service_Standard  = enddate.strftime("%d/%m/%Y")
    

    list_values = [comm_type, ri, company, file_, before_the_board_date, application_date, type_, start_end_order_date, filingid, Service_Standard]
    
    return list_values


###############################Populate forms for Natural Gas orders##############################
# Three templates are being used here:Export Only, Import Only, ExportAndImport                  #
##################################################################################################     
def populate_shortterm_app_form(filingid:str) -> str:  
    today_date = datetime.date.today().strftime("%d %B %Y")
    info_for_mailmerge = gas_order_data(filingid)     
    #Form for Export ONLY or Import ONLY orders 
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
            
##########################################################################################










if __name__ == "__main__":
    
    populate_shortterm_app_form('C03102')

filingid = 'C03102'
today_date = datetime.date.today().strftime("%d %B %Y")
info_for_mailmerge = gas_order_data(filingid)   

info_for_mailmerge = gas_order_data('C03401')    
    #Form for Export ONLY or Import ONLY orders 

####TEST

os.chdir(r'H:\GitHub\import-export-db')   
    
contacts = dce.contact_info('A98680', conn0) 
df_oas = dce.formfields_by_filingId(filingid, conn0)
df_oas[0].columns
df_core = dce.rts_by_filingid(filingid, conn1)
company = df_core[0].LegalName[0]
ctype = dce.application_type(df_oas[0])
gas_order_data('C03367')
dce.comm_type_english_french(df_oas[0])    
####################CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC
os.getcwd()

template = r"H:\GitHub\import-export-db\Import_Export\tmp-final\821263 - NGL NEW Orders Template ENGFR.docx"
document = MailMerge(template)
document.get_merge_fields()
document.close()


dce.commence_end_order_oil(ctype,df_oas[0])





  





















    


    ####ExportANDImport
    elif ctype == "NaturalGasExportANDImport":    
        template = "Import_Export/tmp/Gas/Gas_Export_Import_Orders.docx"
        document_ = MailMerge(template)

        RegInstnum_exp = input("Enter Regulatory Instrument number for exportation:")
        RegInstnum_imp = input("Enter Regulatory Instrument number for importation:")
        
        document_.merge(
        Gas_Type = gas_type_english,
        Type_de_ = gas_type_french,
        
        
        RI_Number_Import = RegInstnum_imp,
        RI_Number_Export = RegInstnum_exp,
        
        Application_Date =  df.ApplicationDate[0] ,
        Before_the_Board_Date = before_the_board_en,
        Company_LegalName = df.LegalName[0],
        RDIMS_FileNum = RDIMSnum,

        DEVANT___lOffice = before_the_board_fr,
        Ex_Order_Commences = ex_commence_date_en,
        Ex_Order_Ends = ex_termination_date_en,
        Ex_Ordre_se_termine = ex_termination_date_fr,
        Ex_en_vigueur_le = ex_commence_date_fr,
        Im_Order_Commences = im_commence_date_en,
        Im_Order_Ends = im_termination_date_en,
        Im_Ordre_se_termine = im_termination_date_fr,
        Im_en_vigueur_le = im_commence_date_fr,        
        
        une_demande__le = une_demande_le_fr,
        
        Filing_ID = FilingID_,
        ServiceStandard = Service_Standard,
        
        Salutation = df.Salutation1[0],
        Name1 = df.FirstName1[0],
        Name2 = df.LastName1[0],
        Salutation_ = df.Salutation1[lngth],
        Name1_ = df.FirstName1[lngth],
        Name2_ = df.LastName1[lngth],
        Title = df.Title1[0],
        Title_ = df.Title1[lngth],
        Company = df.Organization1[0],
        Company_ = df.Organization1[lngth],
        EMail_Address = df.Email1[0],
        EMail_Address_ = df.Email1[lngth],
        rts_link = rts)
        
        document_.write(FilingID_+'_'+RDIMSnum+'_'+today_date+'.docx')
        document_.close()
        
        
###############Populate forms for Light and HeavyCrudeOil, RefinedPetroleum#######################
# Three templates are being used here:Export Only, Import Only, ExportAndImport                  #
################################################################################################## 
elif ctype in ["HeavyCrudeOnlyExport","LightHeavyCrudeOilExport","RefinedPetroleumExport","LightHeavyCrudeOilAndRefinedPetroleumExport"]:
    template = "Import_Export/tmp/Oil/Crude_Oil__Short_Term_Export_Order.docx"
    document = MailMerge(template)
   
    ########################Getting the commodity type################
    if ctype == "LightHeavyCrudeOilAndRefinedPetroleumExport":
        exp_type_en = "Light and Heavy Crude Oil and Refined Petroleum Products"
        
        exp_type_fr = "pétrole brut léger et lourd et produits pétroliers raffinés"
        
    elif ctype == "LightHeavyCrudeOilExport":
        exp_type_en = "Light and Heavy Crude Oil"
        
        exp_type_fr = "pétrole brut léger et lourd"
        
    elif ctype == "RefinedPetroleumExport":
        exp_type_en = "Refined Petroleum Products"
        
        exp_type_fr = "produits pétroliers raffinés"
        
    else:
        exp_type_en = "Heavy crude oil"
        
        exp_type_fr = "pétrole brut lourd"

    
    
    
            
    if df.Export_Commence_Date[0] == "N/A": 
        commence_date_fr = "None"
    else: 
        commence_date_fr = str(df.Export_Commence_Date[0].split()[0]) + ' '+ month_to_french(df.Export_Commence_Date[0].split()[1]) + ' ' + str(df.Export_Commence_Date[0].split()[2]) 
    
    if df.Export_Expiry_Date[0] != "N/A": 
        termination_date_fr = str(df.Export_Expiry_Date[0].split()[0]) + ' '+ month_to_french(df.Export_Expiry_Date[0].split()[1]) + ' ' + str(df.Export_Expiry_Date[0].split()[2]) if df.Export_Expiry_Date[0] is not None else "None"
    else: 
        termination_date_fr = "None"
    

#        commence_date_fr = str(df.Export_Commence_Date[0].split()[0]) + ' '+ month_to_french(df.Export_Commence_Date[0].split()[1]) + ' ' + str(df.Export_Commence_Date[0].split()[2]) 
#        termination_date_fr = str(df.Export_Expiry_Date[0].split()[0]) + ' '+ month_to_french(df.Export_Expiry_Date[0].split()[1]) + ' ' + str(df.Export_Expiry_Date[0].split()[2]) 
    commence_date_en = df.Export_Commence_Date[0]
    termination_date_en= df.Export_Expiry_Date[0]
    
    document.merge(
    ROE_= input("Enter Regulatory Instrument number for exportation:"),
    
    Application_Date =  df.ApplicationDate[0] ,
    Before_the_Board = before_the_board_en,
    Company_LegalName = df.LegalName[0],
    RDIMS_FileNum = RDIMSnum,
    
    Export_Type = exp_type_en,
    Type_du_Export = exp_type_fr,
    
    Devant_lOffice = before_the_board_fr,
    Order_Commences = commence_date_en,
    Order_Terminates = termination_date_en,
    Prendra_fin_le = termination_date_fr,
    En_vigueur_le = commence_date_fr,
    Une_demande_le = une_demande_le_fr,
    
    Filing_ID = FilingID_,
    ServiceStandard = Service_Standard,
    
    Salutation = df.Salutation1[0],
    Name1 = df.FirstName1[0],
    Name2 = df.LastName1[0],
    Salutation_ = df.Salutation1[lngth],
    Name1_ = df.FirstName1[lngth],
    Name2_ = df.LastName1[lngth],
    Title = df.Title1[0],
    Title_ = df.Title1[lngth],
    Company = df.Organization1[0],
    Company_ = df.Organization1[lngth],
    Email_Adress = df.Email1[0],
    EMail_Address_ = df.Email1[lngth],
    rts_link = rts)

    
    document.write(FilingID_+'_'+RDIMSnum+'_'+today_date+'.docx')
    document.close()

