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
import pyodbc


from win32com.client import Dispatch



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
    
    ###Assumption: form before the board on the same day
    today = datetime.date.today()
    # dd/mm/YY
    #today_date = today.strftime("%d %B %Y")
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
        ri = ['GO-XXX-YYYY','GO-XXX-YYYY']
       # ri = 'GO-XXX-YYYY'
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

##############################################################################################################################################################
#input: filingid
#output: populate the english and french version of orders and save them in the working directory by following
#the naming convention that is introduced by Tolls and Tariffs 
##############################################################################################################################################################
def populate_shortterm_app_form(filingid:str) -> list:  
    file_names = list()
    info_for_mailmerge = order_data(filingid) 
    
    #NGL        
    if info_for_mailmerge[0][0] == 'ngl':
        list_of_templates = ['821263 - NGL NEW Orders Template_Propane_Only_EN.docx','821263 - NGL NEW Orders Template_Propane_Only_FR.docx',
                             '821263 - NGL NEW Orders Template_Butanes_Only_EN.docx','821263 - NGL NEW Orders Template_Butanes_Only_FR.docx']
        if info_for_mailmerge[0][1] == 'propane_butanes_export':
            lst_tmp =  list_of_templates
        elif info_for_mailmerge[0][1] == 'propane_export':
            lst_tmp =  list_of_templates[0:2]
        elif info_for_mailmerge[0][1] == 'butanes_export':
            lst_tmp =  list_of_templates[2:]
        for i in range(0,len(lst_tmp)):
            template_p = "Import_Export/tmp-final/New folder/"+lst_tmp[i]
            #print(template_p)
            document = MailMerge(template_p)

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
                 Order_Commences = info_for_mailmerge[7][0] if i == 0 else info_for_mailmerge[7][2],
                 Order_Ends = info_for_mailmerge[7][1] if i == 0 else info_for_mailmerge[7][3],
                 en_vigueur_le =  info_for_mailmerge[7][4] if i == 0 else info_for_mailmerge[7][6],
                 prend_fin_le =  info_for_mailmerge[7][5] if i == 0 else info_for_mailmerge[7][7],

                 #'Salutation',
                 TYPE = info_for_mailmerge[6][0],
                 #'Title',
                 une_demande__le = info_for_mailmerge[5][1] )
            
            file_name = 'DL Walkaround-'+(info_for_mailmerge[1][0] if i in [0,2] else info_for_mailmerge[1][0]) +'-'+info_for_mailmerge[2]+'-'+('Propane Export Order ENG-Duty Panel' if i == 0 else ('Butanes Export Order ENG-Duty Panel' if i ==1 else ('Propane Export Order FR-Duty Panel' if i == 2 else 'Butanes Export Order FR-Duty Panel' )))+'.docx'

            file_names.append(os.path.abspath(file_name))
            
            document.write(file_name)
            document.close()
    #OIL
    elif info_for_mailmerge[0][0] == 'oil':
        lst_tmp = ['847779 - Template  - New Applications - Crude Oil_EN.docx','847779 - Template  - New Applications - Crude Oil_FR.docx']
        for i in range(0,len(lst_tmp)):
            template = "Import_Export/tmp-final/New folder/"+lst_tmp[i]
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
                
            file_name = 'DL Walkaround-'+info_for_mailmerge[1]+'-'+info_for_mailmerge[2]+'-'+('Oil Export Order ENG-Duty to Panel' if i == 0 else 'Oil Export Order FR-Duty to Panel')+'.docx'
            
            file_names.append(os.path.abspath(file_name))
            
            document.write(file_name)
            document.close()
        
    #GAS        
    elif info_for_mailmerge[0][0] == 'gas':
        list_of_templates = ['727292 - TEMPLATE - Gas Export Order_EN.docx','727292 - TEMPLATE - Gas Export Order_FR.docx',
                             '727294 - TEMPLATE - Gas Import Order_EN.docx','727294 - TEMPLATE - Gas Import Order_FR.docx']
        if info_for_mailmerge[0][1] == 'gas_export_import':
            lst_tmp =  list_of_templates
        elif info_for_mailmerge[0][1] == 'gas_export':
            lst_tmp =  list_of_templates[0:2]
        elif info_for_mailmerge[0][1] == 'gas_import':
            lst_tmp =  list_of_templates[2:]
        for i in range(0,len(lst_tmp)):
            template_p = "Import_Export/tmp-final/New folder/"+lst_tmp[i]
            #print(template_p)
            document = MailMerge(template_p)

            document.merge(
                 Application_Date = info_for_mailmerge[5][0],
                 Before__the_Bd_Date = info_for_mailmerge[4][0],
                 Company = info_for_mailmerge[2],
                 DEVANT___lOffice = info_for_mailmerge[4][1],
                 #Date_Sent_to_Walkaround = ,
                 #'EMail_Address',
                 GO_ex = info_for_mailmerge[1][0],
                 GO_im = info_for_mailmerge[1][1],
                 File_ = info_for_mailmerge[3],
                 Filing_ID = info_for_mailmerge[8],
                 GENRE = info_for_mailmerge[6][1],
                 #'Name1',
                 #'Name2',
                 Order_Commences = info_for_mailmerge[7][0] if (i%2) == 0 else info_for_mailmerge[7][2],
                 Order_Ends = info_for_mailmerge[7][1] if (i%2) == 0 else info_for_mailmerge[7][3],
                 en_vigueur_le =  info_for_mailmerge[7][4] if (i%2) == 0 else info_for_mailmerge[7][6],
                 Ordre_se_termine =  info_for_mailmerge[7][5] if (i%2) == 0 else info_for_mailmerge[7][7],

                 #'Salutation',
                 TYPE = info_for_mailmerge[6][0],
                 #'Title',
                 une_demande__le = info_for_mailmerge[5][1] )
            
            file_name = 'DL Walkaround-'+(info_for_mailmerge[1][0] if i in [0,2] else info_for_mailmerge[1][0]) +'-'+info_for_mailmerge[2]+'-'+('Gas Export Order ENG-Duty Panel' if i == 0 else ('Gas Export Order FR-Duty Panel' if i ==1 else ('Gas Import Order ENG-Duty Panel' if i == 2 else 'Gas-Import Order FR-Duty Panel' )))+'.docx'
            
            file_names.append(os.path.abspath(file_name))
            
            document.write(file_name)
            document.close()
            
    return file_names
                
##############################################################################################################################################################
#input: filingid
#output:email sent to the identified Distribution List with english and french versions of the order as the attachment
#the email body takes valiables which change by each application specifications
##############################################################################################################################################################
def email_to_RO(filingid:str) -> str:
    
    msg = str()
    rts_decision = str()
    f_names = populate_shortterm_app_form(filingid) 
    app_info = order_data(filingid)
    
    outlook = Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    
    #fill the email body with following information: filingid, name of the company, 
    #contact information of the applicant, date of the service standard
    filing_id  = filingid
    company = app_info[2]
    service_standard = app_info[9]
    contacts = dce.contact_info(filingid,conn0)
    
    #top part of the email body before contact information    
    msg_body = """<html><body><p>Good morning/afternoon,<br><br><br>Attached is an order for Walkarounds.\
    <br><br>By application e-filed<b> %s</b>, <b>%s</b> has filed an application pursuant to subparagraph 15(a)(i) and section 16 of the National Energy Board Act,\
    Part VI (Oil and Gas) Regulations for a short-term export order under the policy guidelines.The Energy Adjudication Business Unit, Tolls and Tariffs Adjudication Team, recommends that the Commission approve the request and grant the order.</br></br>
    <br><br><b>
    NOTE: A 2- Business day service standard applies. In order to meet our service standard, this needs to be processed by COB on %s.</br></br></b>

    </p></html>""" % (filing_id,company,service_standard)
    
    
    msg = msg_body + msg
    
    #Contact information
    for i in range(0,len(contacts)):
        msg_ =  """<html><body><u><b>%s:</p></u></b>%s %s %s<br>%s</br><br>%s</br><br>%s</br><br><br></html>""" % (contacts.loc[i][1], contacts.loc[i][6] , contacts.loc[i][4], contacts.loc[i][5],contacts.loc[i][7],contacts.loc[i][8],contacts.loc[i][9])
        msg = msg +''+ msg_

    #Ending part of the email
    msg_ending = """<html><body><b><u>RTS<br>
    The RTS link will be provided by the Data Management Team.<br><br>
    
    OR,</b></u><br><br>
    
    RTS screens have been completed and regulatory instrument number(s) have been generated in RTS.  Upon Commission’s approval the RTS Decision Item screen needs to be completed by the Office of the Secretary.  To access directly the RTS Decision Items please click here: %s.<br><br>
    
    Thank you,</html>""" % (rts_decision)
    
    msg = msg + msg_ending    
    
    
    mail.To = 'data_design_analytics@cer-rec.gc.ca'
    mail.Subject = 'This is test # 2 to dispatch emails with attachments'
   # mail.Body = msg
    mail.HTMLBody = msg
   
    # To attach a file to the email (optional):
    for i in range(0,len(f_names)):
        attachment  = f_names[i]
        mail.Attachments.Add(attachment)
    
    mail.Send()
        
    
    
 

    
    
#*************************************TEST**************************     


if __name__ == "__main__":
    filingid = 'C03707'
    email_to_RO(filingid) 
