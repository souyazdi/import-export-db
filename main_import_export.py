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
import data_core_eforms as dce

os.chdir(r'C:\Users\yazdsous\Desktop\GitHub\import-export-database-approach')
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
# input: dataframe from "info_emailbody_shortterm_app" and 
# "shortterm_app_html" respectively
# output: word documents with merged data andsaved in working directory 
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
    ri = 'GO-XXX-YYYY'

    company = df_core[0].LegalName[0]
    
    file_ = df_core[0].FileNumber[0]
    
    before_the_board_date = [before_the_board_en,before_the_board_fr] 
    
    # Application date in french
    application_date_en = df_oas[0].AddedOn[0].strftime("%d %B %Y")
    une_demande_le_fr = dce.date_french(application_date_en)
    application_date = [application_date_en, une_demande_le_fr]
    
    type_ = list(dce.comm_type_english_french(df_oas[0]))
    
    start_end_order_date = list(dce.commence_end_order(ctype, df_oas[0]))
    
    ######Service standard
    enddate = dce.add_business_days(pd.to_datetime(application_date_en),2)
    #enddate = pd.to_datetime(today) + pd.DateOffset(days=2)
    Service_Standard  = enddate.strftime("%d %B %Y")
    

    list_values = [ri, company, file_, before_the_board_date, application_date, type_, start_end_order_date, filingid, Service_Standard]
    
    return list_values




contacts = dce.contact_info('C03398', conn0) 
df_oas = dce.formfields_by_filingId('C03398', conn0)
df_oas[0].columns
df_core = dce.rts_by_filingid('C03398', conn1)
company = df_core[0].LegalName[0]
ctype = dce.application_type(df_oas[0])
gas_order_data('A98680')
    
    
    
    
    
    
    

def populate_shortterm_app_form(filingid:str) -> str:   
###############################Populate forms for NGL orders######################################
# Only Propane and Butanes                                                                       #
##################################################################################################   
    if ctype in ["PropaneOnlyExport","ButanesOnlyExport","PropaneANDButanesExport"]:
        
        
        template = "Import_Export/tmp/NLG/NLG_Export_Orders.docx"
        document = MailMerge(template)
        
        if ctype == "PropaneOnlyExport":
            if df.Propane_Export_Commence_Date[0] == "N/A": 
                commence_date_fr = "None"
                commence_date_en = "None"
            else: 
                commence_date_fr = str(df.Propane_Export_Commence_Date[0].split()[0]) + ' '+ month_to_french(df.Propane_Export_Commence_Date[0].split()[1]) + ' ' + str(df.Propane_Export_Commence_Date[0].split()[2]) 
                commence_date_en =  df.Propane_Export_Commence_Date[0]
                
            if df.Propane_Export_Expiry_Date[0] != "N/A": 
                termination_date_fr = str(df.Propane_Export_Expiry_Date[0].split()[0]) + ' '+ month_to_french(df.Propane_Export_Expiry_Date[0].split()[1]) + ' ' + str(df.Propane_Export_Expiry_Date[0].split()[2]) 
                termination_date_en = df.Propane_Export_Expiry_Date[0]
            else: 
                termination_date_fr = "None"
                termination_date_en = "None"
        else:
            if df.Butanes_Export_Commence_Date[0] == "N/A": 
                commence_date_fr = "None"
                commence_date_en = "None"
            else: 
                commence_date_fr = str(df.Butanes_Export_Commence_Date[0].split()[0]) + ' '+ month_to_french(df.Butanes_Export_Commence_Date[0].split()[1]) + ' ' + str(df.Butanes_Export_Commence_Date[0].split()[2]) 
                commence_date_en = df.Butanes_Export_Commence_Date[0]
                
            if df.Propane_Export_Expiry_Date[0] != "N/A": 
                termination_date_fr = str(df.Butanes_Export_Expiry_Date[0].split()[0]) + ' '+ month_to_french(df.Butanes_Export_Expiry_Date[0].split()[1]) + ' ' + str(df.Butanes_Export_Expiry_Date[0].split()[2]) 
                termination_date_en = df.Butanes_Export_Expiry_Date[0]
            else: 
                termination_date_fr = "None"
                termination_date_en = "None"
#        commence_date_fr = str(df.Export_Commence_Date[0].split()[0]) + ' '+ month_to_french(df.Export_Commence_Date[0].split()[1]) + ' ' + str(df.Export_Commence_Date[0].split()[2]) 
#        termination_date_fr = str(df.Export_Expiry_Date[0].split()[0]) + ' '+ month_to_french(df.Export_Expiry_Date[0].split()[1]) + ' ' + str(df.Export_Expiry_Date[0].split()[2]) 

        
        document.merge(
        Propane_Order = input("Enter Regulatory Instrument number for Propane (if this commodity is not the subject of application, press enter!):"),
        Butanes_Order = input("Enter Regulatory Instrument number for Butanes (if this commodity is not the subject of application, press enter!):"), 
        
        Company_LegalName = df.LegalName[0],
        File_= RDIMSnum,
        Before__the_Bd_Date= before_the_board_en, 
        Application_Date=df.ApplicationDate[0], 
        Order_Commences=commence_date_en,
        Order_Ends=termination_date_en, 

        DEVANT___lOffice = before_the_board_fr,
        une_demande_le = une_demande_le_fr, 
        en_vigueur_le=commence_date_fr,
        prend_fin_le = termination_date_fr,
        
        Filing_ID = FilingID_,
        ServiceStandard = Service_Standard,
        
        
        Title = df.Title1[0],
        Title_ = df.Title1[lngth],
        Salutation = df.Salutation1[0],
        Name1 = df.FirstName1[0],
        Name2 = df.LastName1[0],
        Salutation_ = df.Salutation1[lngth],
        Name1_ = df.FirstName1[lngth],
        Name2_ = df.LastName1[lngth],
        Company = df.Organization1[0],
        Company_ = df.Organization1[lngth],
        EMail_Address = df.Email1[0],
        EMail_Address_ = df.Email1[lngth],
        rts_link = rts )
        
        document.write(FilingID_+'_'+RDIMSnum+'_'+today_date+'.docx')
        document.close()
        
###############################Populate forms for Natural Gas orders##############################
# Three templates are being used here:Export Only, Import Only, ExportAndImport                  #
##################################################################################################        
    elif ctype in ["NaturalGasExportOnly","NaturalGasImportOnly","NaturalGasExportANDImport"]:
        global gas_type_english
        global gas_type_french

        if ctype == "NaturalGasImportOnly": 
            c_fr = str(df.Import_Commence_Date[0].split()[0]) + ' '+ month_to_french(df.Import_Commence_Date[0].split()[1]) + ' ' + str(df.Import_Commence_Date[0].split()[2]) 
            c_en = df.Import_Commence_Date[0]

            t_fr = str(df.Import_Expiry_Date[0].split()[0]) + ' '+ month_to_french(df.Import_Expiry_Date[0].split()[1]) + ' ' + str(df.Import_Expiry_Date[0].split()[2]) if df.Import_Expiry_Date[0] is not None else "None"
            t_en = df.Import_Expiry_Date[0]

        ##############################################################
        #these values are passed to Only Import orders
        elif ctype == "NaturalGasExportOnly": 
            print(df.Export_Commence_Date)
            print(df.Export_Expiry_Date)
            c_fr = str(df.Export_Commence_Date[0].split()[0]) + ' '+ month_to_french(df.Export_Commence_Date[0].split()[1]) + ' ' + str(df.Export_Commence_Date[0].split()[2]) 
            c_en = df.Export_Commence_Date[0]

          
            t_fr = str(df.Export_Expiry_Date[0].split()[0]) + ' '+ month_to_french(df.Export_Expiry_Date[0].split()[1]) + ' ' + str(df.Export_Expiry_Date[0].split()[2]) if df.Export_Expiry_Date[0] is not None else "None"
            t_en = df.Export_Expiry_Date[0]
         

        ##############################################################
        #these values are passed to ExportANDImport orders
        #elif ctype == "NaturalGasExportANDImport": 
        else:
            im_commence_date_fr = str(df.Import_Commence_Date[0].split()[0]) + ' '+ month_to_french(df.Import_Commence_Date[0].split()[1]) + ' ' + str(df.Import_Commence_Date[0].split()[2]) 
            im_commence_date_en = df.Import_Commence_Date[0]
            
            im_termination_date_fr = str(df.Import_Expiry_Date[0].split()[0]) + ' '+ month_to_french(df.Import_Expiry_Date[0].split()[1]) + ' ' + str(df.Import_Expiry_Date[0].split()[2]) if df.Import_Expiry_Date[0] is not None else "None"
            im_termination_date_en = df.Import_Expiry_Date[0]
            
            ex_commence_date_fr = str(df.Export_Commence_Date[0].split()[0]) + ' '+ month_to_french(df.Export_Commence_Date[0].split()[1]) + ' ' + str(df.Export_Commence_Date[0].split()[2]) 
            ex_commence_date_en = df.Export_Commence_Date[0]
            
            ex_termination_date_fr = str(df.Export_Expiry_Date[0].split()[0]) + ' '+ month_to_french(df.Export_Expiry_Date[0].split()[1]) + ' ' + str(df.Export_Expiry_Date[0].split()[2]) if df.Export_Expiry_Date[0] is not None else "None"
            
            ex_termination_date_fr = str(df.Export_Expiry_Date[0].split()[0]) + ' '+ month_to_french(df.Export_Expiry_Date[0].split()[1]) + ' ' + str(df.Export_Expiry_Date[0].split()[2]) if df.Export_Expiry_Date[0] is not None else "None"
            
            ex_termination_date_en = df.Export_Expiry_Date[0]
            
            ##########rmination_date_en = df.Export_Expiry_Date[0]
            
            ##############################################################            
                
        #Form for Export ONLY or Import ONLY orders 
        if ctype != "NaturalGasExportANDImport":
            if ctype == "NaturalGasImportOnly":
                template = "Import_Export/tmp/Gas/Gas_Import_Orders.docx"
            else :
                template = "Import_Export/tmp/Gas/Gas_Export_Orders.docx"
            
            RegInstnum = input("Enter Regulatory Instrument number:")
                
            document = MailMerge(template)
           
            document.merge(
            Gas_Type = gas_type_english,
            Type_de_Gaz = gas_type_french,
            
            
            RI_Number_Import = RegInstnum,
            RI_Number_Export = RegInstnum,
            
            Application_Date =  df.ApplicationDate[0] ,
            Before_the_Board_Date = before_the_board_en,
            Company_LegalName = df.LegalName[0],
            RDIMS_FileNum = RDIMSnum,
    
            DEVANT___lOffice = before_the_board_fr,
            Order_Commences = c_en,
            Order_Ends = t_en,
            Ordre_se_termine = t_fr,
            en_vigueur_le = c_fr,
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
            
            document.write(FilingID_+'_'+RDIMSnum+'_'+today_date+'.docx')
            document.close()
        
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

