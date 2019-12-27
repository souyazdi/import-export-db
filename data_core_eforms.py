# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 10:11:28 2019

@author: yazdsous
"""
import numpy as np
import pyodbc
import pandas as pd


import datetime 

#conn = pyodbc.connect('Driver={SQL Server};'
#                      'Server=DSQL23CAP;'
#                      'Database=Regulatory_Untrusted;'
#                      'Trusted_Connection=yes;')

conn0 = pyodbc.connect('Driver={SQL Server};'
                      'Server=Hosaka\Sqlp2;'
                      'Database=Eforms;'
                      'Trusted_Connection=yes;')

conn1 = pyodbc.connect('Driver={SQL Server};'
                      'Server=ndb-a1;'
                      'Database=Core;'
                      'Trusted_Connection=yes;')

#cursor = conn.cursor()


#query_gas = "SELECT [FormId]\
#      ,[Name]\
#      ,[FilingId]\
#      ,[AddedOn]\
#  FROM [Eforms].[dbo].[Form]\
#  WHERE FilingID IS NOT NULL AND [Name] = N's15ab_ShrtTrmNtrlGs_ImprtExprt'\
#  ORDER BY FormId DESC"

###############################################################################
#This function accepts the filigid of the application + the pyodbc object 
#joins the Form and FormField tables from Eform DB (on FormId) and returns the 
#corresponding dataframe with formId, Name, FilingIdm, ASPFieldIdName, and
#ASPFiledIdValue
#Output: A tuple with (Dataframe, FormId)
###############################################################################
def formfields_by_filingId(filingid:str, conn0) -> pd.DataFrame:
        query = "SELECT f.[FormId]\
        ,f.[AddedOn]\
        ,[Name]\
        ,[FilingId]\
        ,[ASPFieldIdName]\
        ,[ASPFieldIdValue]\
        FROM [Eforms].[dbo].[Form] f\
        JOIN [FormField] ff\
        ON f.FormId = ff.FormId\
        WHERE FilingId IS NOT NULL AND [FilingId] = \'{}\'\
        ORDER BY ff.FormId DESC".format(filingid)
        df_filingid = pd.read_sql(query,conn0)
        return df_filingid, df_filingid.FormId[0]
###############################################################################
###############################################################################
#This function accepts the FormId of the application + the pyodbc object 
#and extracts the contact information filled by the applicant.
#Output: A dataframe with the information corresponding to each contact type
#for the application (with the FormId passed as the argument)
###############################################################################
def contact_info(filingid:str, conn0) -> pd.DataFrame:
    query = "SELECT [ContactId]\
    ,ct.Name Contact_Type\
	,ct.ContactTypeId\
    ,[FormId]\
    ,[FirstName]\
    ,[LastName]\
    ,[Salutation]\
    ,[Title]\
    ,[Organization]\
    ,[Email]\
    ,Country.Name Country\
    ,Province.Name Province\
    ,[Address]\
    ,[City]\
    ,[PostalCode]\
    ,[PhoneNumber]\
    ,[PhoneExt]\
    ,[FaxNumber]\
    FROM [Eforms].[dbo].[Contact] c\
    JOIN ContactType ct\
    ON c.ContactTypeId = ct.ContactTypeId\
    JOIN Country\
    ON Country.CountryId = c.CountryId\
    JOIN Province\
    ON Province.ProvinceId = c.ProvinceId WHERE FormId = (SELECT FormId FROM [Eforms].[dbo].[Form] WHERE FilingId = \'{}\')".format(filingid)
    df_fid = pd.read_sql(query,conn0)
    return df_fid
###############################################################################
###############################################################################
#Input: FilingId of the application + the pyodbc object 
#Output: A dataframe with the information in CORE corresponding to the apps
#joins of CORE tables and filtering by the FilingId
###############################################################################    
def rts_by_filingid(filingid:str, conn1) -> pd.DataFrame:
    query = "SELECT f.[FileId], f.[FileNumber],f.[RecordsTitle],f.[RecordsDescription],\
	a.[ActivityId],a.[EnglishTitle],a.[FrenchTitle],a.[Description] ActivityDescription,\
    a.[ApplicationDate],a.[ReceivedDate],a.[ExpectedCompletionDate],a.[InternalProjectFlag],\
    a.[StatusId],a.[CompletedDate],a.[DeactivationDate] ActivityDeactivationDate,\
    a.[LegacyProjectXKey],a.[LetterForCommentFlag],a.[LetterForCommentExpireDate],\
    a.[BusinessUnitId],a.[FederalLandId],a.[DecisionOnCompleteness],a.[EnglishProjectShortName],\
    a.[FrenchProjectShortName],a.[FrenchDescription] FrenchDescriptionOfActivity,\
	aa.[ActivityAttachmentId] ,aa.[LivelinkCompoundDocumentId],\
	be.[BoardEventId] ,be.[NextTimeToBoard] ,be.[PurposeId],be.[Description],\
    be.[PrimaryContactId],be.[SecondaryContactId] ,\
	di.[DecisionItemId],di.[DecisionItemStatusId],di.[RegulatoryInstrumentNumber],\
    di.[IssuedDate],di.[EffectiveDate],di.[ExpireDate],di.[SunsetDate],\
    di.[IssuedToNonRegulatedCompanyFlag],di.[LetterOnly],di.[Comments],di.[AddedBy],\
    di.[AddedOn],di.[ModifiedBy],di.[ModifiedOn],di.[WalkaroundFolderId],\
    di.[BoardDecisionDate],di.[ReasonForCancelling],di.[GicApproval],di.[SentToMinisterDate],\
    di.[MinisterToPrivyCouncilOfficeDate],di.[PrivyCouncilOfficeApprovalNumber],\
    di.[PrivyCouncilOfficeApprovalDate],di.[RegulatoryOfficerAssignedId],di.[IsNGLLicence],\
    di.[FrenchComments],fc.[FileCompanyId] ,fc.[CompanyId],\
	c.[CompanyId] CompanyIdC,c.[CompanyCode] ,c.[LegalName],c.[DeactivationDate],c.[IsGroup1]\
	FROM [File] f\
	JOIN [Activity] a\
	ON f.FileId = a.FileId\
	JOIN [ActivityAttachment] aa\
	ON a.ActivityId = aa.ActivityId\
	FULL JOIN [BoardEvent] be\
	ON be.ActivityId = a.ActivityId\
	FULL JOIN [DecisionItem] di\
	ON be.BoardEventId = di.BoardEventId\
	JOIN [FileCompany] fc \
    ON fc.FileId = a.FileId\
    JOIN [Company] c\
	ON c.CompanyId = fc.CompanyId\
	WHERE aa.LivelinkCompoundDocumentId = \'{}\'".format(filingid)
    df_filingid = pd.read_sql(query,conn1)
    return df_filingid, df_filingid.shape[0]
###############################################################################
###############################################################################
#A DATAFRAME is passed as the argument to this function. Input Dataframe is the
#output of function formfileds_by_filingId(...)
#Output: Commodity type (one commodity or a multiple commodities, depending on 
# the application) and whether it is export or in the case of gas applications
#it is export or both
###############################################################################
def application_type(df:pd.DataFrame) -> str:
    try:
        #GAS
        app_name = df.Name[0]
        df_fields = df.loc[:,['ASPFieldIdName','ASPFieldIdValue']]
        if app_name == 's15ab_ShrtTrmNtrlGs_ImprtExprt':
            gas_import = df_fields.loc[df_fields['ASPFieldIdName'] == 'chkbx_s15ab_ShrtTrmNtrlGs_ImprtExprt_Athrztns_ImportOrder','ASPFieldIdValue'].values[0]
            gas_export = df_fields.loc[df_fields['ASPFieldIdName'] == 'chkbx_s15ab_ShrtTrmNtrlGs_ImprtExprt_Athrztns_ExportOrder','ASPFieldIdValue'].values[0]
            if all(map((lambda value: value == 'True'), (gas_import,gas_export))):
                return 'gas','gas_export_import'
            elif gas_import == 'False' and gas_export == 'True':
                return 'gas','gas_export'
            elif gas_import == 'True' and gas_export == 'False':
                return 'gas','gas_import'
        #NGL    
        elif app_name == 's22_ShrtTrmNgl_Exprt':
            propane_export = df_fields.loc[df_fields['ASPFieldIdName'] == 'chkbx_s22_ShrtTrmNgl_Exprt_Athrztns_ProductType_Propane','ASPFieldIdValue'].values[0]
            butanes_export = df_fields.loc[df_fields['ASPFieldIdName'] == 'chkbx_s22_ShrtTrmNgl_Exprt_Athrztns_ProductType_Butanes','ASPFieldIdValue'].values[0]
            if all(map((lambda value: value == 'True'), (propane_export,butanes_export))):
                return 'ngl','propane_butanes_export'
            elif propane_export == 'False' and butanes_export == 'True':
                return 'ngl','butanes_export'
            elif propane_export == 'True' and butanes_export == 'False':
                return 'ngl','propane_export'
        #OIL
        elif app_name == 's28_ShrtTrmLghtHvCrdRfnd_Exprt':    
            light_heavy_crude_export = df_fields.loc[df_fields['ASPFieldIdName'] == 'chkbx_s28_ShrtTrmLghtHvCrdRfnd_Exprt_Athrztns_HeavyCrude','ASPFieldIdValue'].values[0]
            refined_products_export = df_fields.loc[df_fields['ASPFieldIdName'] == 'chkbx_s28_ShrtTrmLghtHvCrdRfnd_Exprt_Athrztns_RefinedProducts','ASPFieldIdValue'].values[0]
            if all(map((lambda value: value == 'True'), (light_heavy_crude_export,refined_products_export))):
                return 'oil','lightheavycrude_refinedproducts_export'
            elif light_heavy_crude_export == 'False' and refined_products_export == 'True':
                return 'oil','lightheavycrude_export'
            elif light_heavy_crude_export == 'True' and refined_products_export == 'False':
                return 'oil','refinedproducts_export'      
        elif app_name == 's28_ShrtTrmHvCrd_Exprt': 
            return 'oil','heavycrude_export'
   
        else:       
            return 'this is not a gas, ngl, or oil order'
    except ValueError:
        return 'Value'
    except TypeError:
        return 'Type'
    
###############################################################################  
#           NOTE:
###############################################################################
#GasType -> 1 -> Natural Gas
#GasType -> 2 -> Natural Gas, in the form of Liquefied Natural Gas
#GasType -> 3 -> Natural Gas, in the form of Compressed Natural Gas
###############################################################################
#Input: Commodity name in english 
#Output: Commodity name in French
###############################################################################   
def comm_type_english_french(df:pd.DataFrame) -> list:
    try:
        if application_type(df)[0] == 'gas':
            gas_en,gas_fr = str(),str()
            gas_type = df.loc[df['ASPFieldIdName'] == 'rbl_s15ab_ShrtTrmNtrlGs_ImprtExprt_Athrztns_ExportOrder_GasType','ASPFieldIdValue'].values[0]
            if gas_type == '2':
                gas_en = 'natural gas, in the form of Liquefied Natural Gas'
                gas_fr = 'gaz, sous la forme de gaz naturel liquéfié seulement'
            elif gas_type == '3':
                gas_en = 'natural gas, in the form of compressed natural gas'
                gas_fr = 'gaz, sous la forme de gaz naturel comprimé'    
            return gas_en , gas_fr
        
        if application_type(df)[0] == 'oil':
            oil_en,oil_fr = str(),str()
            oil_type = application_type(df)[1]
            if oil_type == 'lightheavycrude_refinedproducts_export':
                oil_en = 'light and heavy crude oil and pefined petroleum products'
                oil_fr = 'pétrole brut léger et lourd et produits pétroliers raffinés'
                
            elif oil_type == 'lightheavycrude_export':
                oil_en = 'light and heavy crude oil'
                oil_fr = 'pétrole brut léger et lourd'
                
            elif oil_type == 'refinedproducts_export':
                oil_en = 'refined petroleum products'
                oil_fr = 'produits pétroliers raffinés'   
                
            elif oil_type == 'heavycrude_export':
                oil_en = 'heavy crude oil'
                oil_fr = 'pétrole brut lourd'             
            return oil_en , oil_fr
        
        if application_type(df)[0] == 'ngl':
            ngl_en,ngl_fr = str(),str()
            return ngl_en , ngl_fr
        
        else:
            return ('other comms....')
            exit
        
    except ValueError:
        return 'Value'
    except TypeError:
        return 'Type'
#**************************************************************************************************
# input: month of the year in English in full version
# output: French months
# This function converts English months to French
#**************************************************************************************************
def month_to_french(month): 
    fr_months = ['janvier','février','mars','avril','mai','juin','juillet','août','septembre','octobre','novembre','décembre']
    switcher = { 
        "January": fr_months[0], 
        "February": fr_months[1], 
        "March": fr_months[2], 
        "April": fr_months[3],
        "May": fr_months[4], 
        "June": fr_months[5],
        "July": fr_months[6], 
        "August": fr_months[7],
        "September": fr_months[8], 
        "October": fr_months[9],
        "November": fr_months[10], 
        "December": fr_months[11],   
    } 
  
    # get() method of dictionary data type returns  
    # value of passed argument if it is present  
    # in dictionary otherwise second argument will 
    # be assigned as default value of passed argument 
    return switcher.get(month, "nothing") 
#**************************************************************************************************
# input: Date in the form of XX Month(English) XXXX
# output: French version
# This function converts English date to French
#**************************************************************************************************
def date_french(date_en:str)-> str:
    try:
        return(date_en.split()[0]) + ' '+ month_to_french(date_en.split()[1]) + ' ' + str(date_en.split()[2])
    except ValueError:
        return 'Value'
    except TypeError:
        return 'Type'
    except:
        return 'Wrong date format'
    
#**************************************************************************************************
#Skip the Weekends
#refernce: https://stackoverflow.com/questions/12691551/add-n-business-days-to-a-given-date-ignoring-holidays-and-weekends-in-python/23352801
#**************************************************************************************************
def add_business_days(from_date, ndays):
    business_days_to_add = abs(ndays)
    current_date = from_date
    sign = ndays/abs(ndays)
    while business_days_to_add > 0:
        current_date += datetime.timedelta(sign * 1)
        weekday = current_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        business_days_to_add -= 1
    return current_date
###############################################################################
#Input: index[0] of output tuple function formfileds_by_filingId(...)
#Output: Order start and end date
###############################################################################              
def commence_end_order_gas(ctype:str, df:pd.DataFrame) -> list:
    export_order_commence_date = str()
    export_order_termination_date = str()
    import_order_commence_date =str()
    import_order_termination_date = str()
    export_order_commence_date_fr = str()
    export_order_termination_date_fr = str()
    import_order_commence_date_fr = str()
    import_order_termination_date_fr = str()
    
    dt = df.AddedOn[0].date()
    application_date = dt.strftime("%d %B %Y")
    
    try:
        if ctype[0] == 'gas':
            #For a period of two years less one day commencing upon approval of the Board
            if df.loc[df['ASPFieldIdName'] == 'rbl_s15ab_ShrtTrmNtrlGs_ImprtExprt_Athrztns_ExportOrder_TimeFrame','ASPFieldIdValue'].values[0] == '1':
                #commences the day after application received date
                ex_order_commence_date = add_business_days(pd.to_datetime(application_date),2)
                export_order_commence_date = ex_order_commence_date.strftime("%d %B %Y")
                export_order_commence_date_fr = date_french(export_order_commence_date) if len(export_order_commence_date.split()) == 3 else 'NULL'
                
                ex_order_termination_date = ex_order_commence_date + pd.DateOffset(years=2) - pd.DateOffset(days=1)
                export_order_termination_date = ex_order_termination_date.strftime("%d %B %Y")
                export_order_termination_date_fr = date_french(export_order_termination_date) if len(export_order_termination_date.split()) == 3 else 'NULL'
            
            if df.loc[df['ASPFieldIdName'] == 'rbl_s15ab_ShrtTrmNtrlGs_ImprtExprt_Athrztns_ImportOrder_TimeFrame','ASPFieldIdValue'].values[0] == '1':
                #commences the day after application received date
                im_order_commence_date = add_business_days(pd.to_datetime(application_date),2)
                import_order_commence_date = im_order_commence_date.strftime("%d %B %Y")
                import_order_commence_date_fr = date_french(import_order_commence_date) if len(import_order_commence_date.split()) == 3 else 'NULL'

                im_order_termination_date = im_order_commence_date + pd.DateOffset(years=2) - pd.DateOffset(days=1)
                import_order_termination_date = im_order_termination_date.strftime("%d %B %Y")
                import_order_termination_date_fr = date_french(import_order_termination_date) if len(import_order_termination_date.split()) == 3 else 'NULL'
            
            if df.loc[df['ASPFieldIdName'] == 'rbl_s15ab_ShrtTrmNtrlGs_ImprtExprt_Athrztns_ExportOrder_TimeFrame','ASPFieldIdValue'].values[0] == '2':
                ex_order_commence_date = df.loc[df['ASPFieldIdName'] == 'txt_s15ab_ShrtTrmNtrlGs_ImprtExprt_Athrztns_ExportOrder_TimeFrame_2_StartDate','ASPFieldIdValue'].values[0]
                export_order_commence_date = pd.to_datetime(ex_order_commence_date).strftime("%d %B %Y") if ex_order_commence_date != 'NULL' else 'NULL'
                export_order_commence_date_fr = date_french(export_order_commence_date) if len(export_order_commence_date.split()) == 3 else 'NULL'

                ex_order_termination_date = df.loc[df['ASPFieldIdName'] == 'txt_s15ab_ShrtTrmNtrlGs_ImprtExprt_Athrztns_ExportOrder_TimeFrame_2_EndDate','ASPFieldIdValue'].values[0]
                export_order_termination_date = pd.to_datetime(ex_order_termination_date).strftime("%d %B %Y") if ex_order_termination_date != 'NULL' else 'NULL'
                export_order_termination_date_fr = date_french(export_order_termination_date) if len(export_order_termination_date.split()) == 3 else 'NULL'

            if df.loc[df['ASPFieldIdName'] == 'rbl_s15ab_ShrtTrmNtrlGs_ImprtExprt_Athrztns_ImportOrder_TimeFrame','ASPFieldIdValue'].values[0] == '2':
                im_order_commence_date = df.loc[df['ASPFieldIdName'] == 'txt_s15ab_ShrtTrmNtrlGs_ImprtExprt_Athrztns_ImportOrder_TimeFrame_2_StartDate','ASPFieldIdValue'].values[0]
                import_order_commence_date = pd.to_datetime(im_order_commence_date).strftime("%d %B %Y") if im_order_commence_date != 'NULL' else 'NULL'
                import_order_commence_date_fr = date_french(import_order_commence_date) if len(import_order_commence_date.split()) == 3 else 'NULL'

                im_order_termination_date = df.loc[df['ASPFieldIdName'] == 'txt_s15ab_ShrtTrmNtrlGs_ImprtExprt_Athrztns_ImportOrder_TimeFrame_2_EndDate','ASPFieldIdValue'].values[0]
                import_order_termination_date = pd.to_datetime(im_order_termination_date).strftime("%d %B %Y") if im_order_commence_date != 'NULL' else 'NULL'
                import_order_termination_date_fr = date_french(import_order_termination_date) if len(import_order_termination_date.split()) == 3 else 'NULL'

        return export_order_commence_date, export_order_termination_date, import_order_commence_date, import_order_termination_date, export_order_commence_date_fr, export_order_termination_date_fr, import_order_commence_date_fr, import_order_termination_date_fr
    
    except:
        return "ERROR:commence_end_order"
        pass

###############################################################################
        #NGL
###############################################################################
def commence_end_order_ngl(ctype:str, df:pd.DataFrame) -> list:
    propane_order_commence_date = str()
    propane_order_termination_date = str()
    butanes_order_commence_date =str()
    butanes_order_termination_date = str()
    propane_order_commence_date_fr = str()
    propane_order_termination_date_fr = str()
    butanes_order_commence_date_fr = str()
    butanes_order_termination_date_fr = str()
    
    dt = df.AddedOn[0].date()
    application_date = dt.strftime("%d %B %Y")
    today = datetime.date.today()
    current_year = today.year
    
    try:
        if ctype[0] == 'ngl':
            #For a period of two years less one day commencing upon approval of the Board
            if df.loc[df['ASPFieldIdName'] == 'rbl_s22_ShrtTrmNgl_Exprt_Athrztns_ProductType_Propane','ASPFieldIdValue'].values[0] == '1':
                #commences the day after application received date
                p_order_commence_date = add_business_days(pd.to_datetime(application_date),1)
                propane_order_commence_date = p_order_commence_date.strftime("%d %B %Y")
                propane_order_commence_date_fr = date_french(propane_order_commence_date) if len(propane_order_commence_date.split()) == 3 else 'NULL'
                
                #until December 31st of current calander year
                p_order_termination_date = datetime.datetime(current_year, 12, 31)
                propane_order_termination_date = p_order_termination_date.strftime("%d %B %Y")
                propane_order_termination_date_fr = date_french(propane_order_termination_date) if len(propane_order_termination_date.split()) == 3 else 'NULL'
            
            if df.loc[df['ASPFieldIdName'] == 'rbl_s22_ShrtTrmNgl_Exprt_Athrztns_ProductType_Butanes','ASPFieldIdValue'].values[0] == '1':
                #commences the day after application received date
                b_order_commence_date = add_business_days(pd.to_datetime(application_date),1)
                butanes_order_commence_date = b_order_commence_date.strftime("%d %B %Y")
                butanes_order_commence_date_fr = date_french(butanes_order_commence_date) if len(butanes_order_commence_date.split()) == 3 else 'NULL'
                
                #until December 31st of current calander year
                b_order_termination_date = datetime.datetime(current_year, 12, 31)
                butanes_order_termination_date = b_order_termination_date.strftime("%d %B %Y")
                butanes_order_termination_date_fr = date_french(butanes_order_termination_date) if len(butanes_order_termination_date.split()) == 3 else 'NULL'
            
            if df.loc[df['ASPFieldIdName'] == 'rbl_s22_ShrtTrmNgl_Exprt_Athrztns_ProductType_Propane','ASPFieldIdValue'].values[0] == '2':
                  #commnences on January 1st of next calander year
                    p_order_commence_date = datetime.datetime(current_year+1, 1, 1)
                    propane_order_commence_date = p_order_commence_date.strftime("%d %B %Y")
                    propane_order_commence_date_fr = date_french(propane_order_commence_date) if len(propane_order_commence_date.split()) == 3 else 'NULL'
                    
                    #until December 31st of next two calander year
                    p_order_termination_date = datetime.datetime(current_year+1, 12, 31)
                    propane_order_termination_date = p_order_termination_date.strftime("%d %B %Y")
                    propane_order_termination_date_fr = date_french(propane_order_termination_date) if len(propane_order_termination_date.split()) == 3 else 'NULL'                                    

            if df.loc[df['ASPFieldIdName'] == 'rbl_s22_ShrtTrmNgl_Exprt_Athrztns_ProductType_Butanes','ASPFieldIdValue'].values[0] == '2':
                  #commnences on January 1st of next calander year
                    b_orde_commences_date = datetime.datetime(current_year+1, 1, 1)
                    butanes_order_commence_date = b_orde_commences_date.strftime("%d %B %Y")
                    butanes_order_commence_date_fr = date_french(butanes_order_commence_date) if len(butanes_order_commence_date.split()) == 3 else 'NULL'
                    
                    #until December 31st of next two calander year
                    b_order_termination_date = datetime.datetime(current_year+1, 12, 31) 
                    butanes_order_termination_date = b_order_termination_date.strftime("%d %B %Y")
                    butanes_order_termination_date_fr = date_french(butanes_order_termination_date) if len(butanes_order_termination_date.split()) == 3 else 'NULL'
                    
        return propane_order_commence_date, propane_order_termination_date, butanes_order_commence_date, butanes_order_termination_date, propane_order_commence_date_fr, propane_order_termination_date_fr, butanes_order_commence_date_fr, butanes_order_termination_date_fr
    
    except:
        return "ERROR:commence_end_order"
        pass

###############################################################################
        #OIL
###############################################################################
def commence_end_order_oil(ctype:str, df:pd.DataFrame) -> list:
    oil_order_commence_date = str()
    oil_order_termination_date = str()
    oil_order_commence_date_fr = str()
    oil_order_termination_date_fr = str()
    
    dt = df.AddedOn[0].date()
    application_date = dt.strftime("%d %B %Y")
    today = datetime.date.today()
    current_year = today.year
    
    try:
        #Application for Short-Term Heavy Crude Only Export Order
        if ctype[0] == 'oil' and ctype[1] == 'heavycrude_export':   
            if df.loc[df['ASPFieldIdName'] == 'rbl_s28_ShrtTrmHvCrd_Exprt_Athrztns_ProductType_HeavyCrude','ASPFieldIdValue'].values[0] == '1':
                #commences one business day after the application date
                order_commences_date = add_business_days(pd.to_datetime(application_date),1)
                oil_order_commence_date = order_commences_date.strftime("%d %B %Y")
                oil_order_commence_date_fr = date_french(oil_order_commence_date) if len(oil_order_commence_date.split()) == 3 else 'NULL'
                
                #until December 31st of next calander year
                order_terminates_date = datetime.datetime(current_year+1, 12, 31) 
                oil_order_termination_date = order_terminates_date.strftime("%d %B %Y")
                oil_order_termination_date_fr = date_french(oil_order_termination_date) if len(oil_order_termination_date.split()) == 3 else 'NULL'
                
            elif df.loc[df['ASPFieldIdName'] == 'rbl_s28_ShrtTrmHvCrd_Exprt_Athrztns_ProductType_HeavyCrude','ASPFieldIdValue'].values[0] == '2':
                #commnences on January 1st of next calander year
                order_commences_date = datetime.datetime(current_year+1, 1, 1)
                oil_order_commence_date = order_commences_date.strftime("%d %B %Y")
                oil_order_commence_date_fr = date_french(oil_order_commence_date) if len(oil_order_commence_date.split()) == 3 else 'NULL'

                #until December 31st of next two calander year
                order_terminates_date = datetime.datetime(current_year+2, 12, 31) 
                oil_order_termination_date = order_terminates_date.strftime("%d %B %Y")
                oil_order_termination_date_fr = date_french(oil_order_termination_date) if len(oil_order_termination_date.split()) == 3 else 'NULL'
            
        elif ctype[0] == 'oil' and ctype[1] != 'heavycrude_export':      
            #For a period of two years less one day commencing upon approval of the Board
            if df.loc[df['ASPFieldIdName'] == 'rbl_s28_ShrtTrmLghtHvCrdRfnd_Exprt_Athrztns_ProductType_HeavyCrude','ASPFieldIdValue'].values[0] == '1' or df.loc[df['ASPFieldIdName'] == 'rbl_s28_ShrtTrmLghtHvCrdRfnd_Exprt_Athrztns_ProductType_RefinedProducts','ASPFieldIdValue'].values[0] == '1':
                #commences one business day after the application date
                order_commences_date = add_business_days(pd.to_datetime(application_date),1)
                oil_order_commence_date = order_commences_date.strftime("%d %B %Y")
                oil_order_commence_date_fr = date_french(oil_order_commence_date) if len(oil_order_commence_date.split()) == 3 else 'NULL'
                
                #until December 31st of next calander year
                order_terminates_date = datetime.datetime(current_year+1, 12, 31) 
                oil_order_termination_date = order_terminates_date.strftime("%d %B %Y")
                oil_order_termination_date_fr = date_french(oil_order_termination_date) if len(oil_order_termination_date.split()) == 3 else 'NULL'
            
            elif df.loc[df['ASPFieldIdName'] == 'rbl_s28_ShrtTrmLghtHvCrdRfnd_Exprt_Athrztns_ProductType_HeavyCrude','ASPFieldIdValue'].values[0] == '2' or df.loc[df['ASPFieldIdName'] == 'rbl_s28_ShrtTrmLghtHvCrdRfnd_Exprt_Athrztns_ProductType_RefinedProducts','ASPFieldIdValue'].values[0] == '2':
                #commnences on January 1st of next calander year
                order_commences_date = datetime.datetime(current_year+1, 1, 1)
                oil_order_commence_date = order_commences_date.strftime("%d %B %Y")
                oil_order_commence_date_fr = date_french(oil_order_commence_date) if len(oil_order_commence_date.split()) == 3 else 'NULL'

                #until December 31st of next two calander year
                order_terminates_date = datetime.datetime(current_year+1, 12, 31) 
                oil_order_termination_date = order_terminates_date.strftime("%d %B %Y")
                oil_order_termination_date_fr = date_french(oil_order_termination_date) if len(oil_order_termination_date.split()) == 3 else 'NULL'
                   
        return oil_order_commence_date, oil_order_termination_date, oil_order_commence_date_fr, oil_order_termination_date_fr
    
    except:
        return "ERROR:commence_end_order"
        pass








#TEST    
filingid = 'C03515'
df_oas = formfields_by_filingId(filingid, conn0)
ct = application_type(formfields_by_filingId(filingid,conn0)[0])
commence_end_order_ngl(ct, df_oas[0])





    
def query_oas_commodity(comtype:str) -> str:
    if comtype.lower() == "gas":
        text = "\'s15ab_ShrtTrmNtrlGs_ImprtExprt\'"
        query_gas = "SELECT f.[FormId]\
        ,[Name]\
        ,[FilingId]\
        ,[ASPFieldIdName]\
        ,[ASPFieldIdValue]\
        FROM [Eforms].[dbo].[Form] f\
        JOIN [FormField] ff\
        ON f.FormId = ff.FormId\
        WHERE FilingId IS NOT NULL AND [Name] = {}\
        ORDER BY ff.FormId DESC".format(text)
    return query_gas

  
df_gas = pd.read_sql(query_oas_commodity('gas'),conn0)
df_gas = df_gas.sort_values(by=['FormId'], ascending = False)  
    

query = 'SELECT * \
    FROM\
    (\
    	SELECT f.[FileId], f.[ParentFileId],f.[FileNumber],f.[RecordsTitle],f.[RecordsDescription],\
    	a.[ActivityId],a.[EnglishTitle],a.[FrenchTitle],a.[Description] ActivityDescription,a.[ApplicationDate],a.[ReceivedDate],a.[ExpectedCompletionDate],a.[InternalProjectFlag],a.[StatusId],a.[CompletedDate],a.[DeactivationDate] ActivityDeactivationDate,a.[LegacyProjectXKey],a.[LetterForCommentFlag],a.[LetterForCommentExpireDate],a.[BusinessUnitId],a.[FederalLandId],a.[DecisionOnCompleteness],a.[EnglishProjectShortName],a.[FrenchProjectShortName],a.[FrenchDescription] FrenchDescriptionOfActivity,\
        aa.[ActivityAttachmentId] ,aa.[LivelinkCompoundDocumentId],\
    	be.[BoardEventId] ,be.[NextTimeToBoard] ,be.[PurposeId],be.[Description],be.[PrimaryContactId],be.[SecondaryContactId] ,be.[LegacySummaryXKey],be.[FrenchDescription] FrenchDescriptionOfBoardEvent,\
    	di.[DecisionItemId],di.[DecisionItemStatusId],di.[RegulatoryInstrumentNumber],di.[IssuedDate],di.[EffectiveDate],di.[ExpireDate],di.[SunsetDate],di.[IssuedToNonRegulatedCompanyFlag],di.[LetterOnly],di.[Comments],di.[LegacyRegulatoryInstrumentXKey],di.[AddedBy],di.[AddedOn],di.[ModifiedBy],di.[ModifiedOn],di.[WalkaroundFolderId],di.[BoardDecisionDate],di.[ReasonForCancelling],di.[GicApproval],di.[SentToMinisterDate],di.[MinisterToPrivyCouncilOfficeDate],di.[PrivyCouncilOfficeApprovalNumber],di.[PrivyCouncilOfficeApprovalDate],di.[RegulatoryOfficerAssignedId],di.[IsNGLLicence],di.[FrenchComments],\
    	fc.[FileCompanyId] ,fc.[CompanyId],\
    	c.[CompanyId] CompanyIdC,c.[CompanyCode] ,c.[LegalName],c.[InPID],c.[InXING],c.[LegacyCompanyXKey] ,c.[DeactivationDate],c.[IsGroup1]\
    	FROM [Regulatory_Untrusted].[_Core].[File] f\
    	JOIN [Regulatory_Untrusted].[_Core].[Activity] a\
    	ON f.FileId = a.FileId\
    	JOIN [Regulatory_Untrusted].[_Core].[ActivityAttachment] aa\
    	ON a.ActivityId = aa.ActivityId\
    	FULL JOIN [Regulatory_Untrusted].[_Core].[BoardEvent] be\
    	ON be.ActivityId = a.ActivityId\
    	FULL JOIN [Regulatory_Untrusted].[_Core].[DecisionItem] di\
    	ON be.BoardEventId = di.BoardEventId\
    	JOIN [Regulatory_Untrusted].[_Core].[FileCompany] fc \
    	ON fc.FileId = a.FileId \
    	JOIN [Regulatory_Untrusted].[_Core].[Company] c \
    	ON c.CompanyId = fc.CompanyId \
    ) as core\
    JOIN\
    (\
    	SELECT f.[FormId],f.[Name] Form_Name,f.[FilingId], f.[AddedOn] FileAddedOn, ff.[FormFieldId],ff.[ASPFieldIdName],ff.[ASPFieldIdValue]\
    	,c.[ContactId],c.[ContactTypeId],c.[FirstName],c.[LastName],c.[Salutation],c.[Title],c.[Role],c.[Organization],c.[Email],c.[CountryId],c.[ProvinceId],c.[Address],c.[City],c.[PostalCode],c.[PhoneNumber],c.[PhoneExt],c.[FaxNumber],c.[AddedBy],c.[AddedOn],c.[ModifiedBy],c.[ModifiedOn],c.[CountryOther],c.[ProvinceOther],\
    	cnt.[Name],p.[Name] ProvinceName, p.[NameFR], p.[Abbreviation]\
    	FROM [Regulatory_Untrusted].[_Eforms].[Form] f \
    	JOIN [Regulatory_Untrusted].[_Eforms].[FormField] ff \
    	ON f.FormId = ff.FormId \
    	JOIN [Regulatory_Untrusted].[_Eforms].[Contact] c\
    	ON f.FormId = c.FormId\
    	JOIN [Regulatory_Untrusted].[_Eforms].[ContactType] ct\
    	ON ct.ContactTypeId = c.ContactTypeId\
    	JOIN [Regulatory_Untrusted].[_Eforms].[Country] cnt\
    	ON cnt.CountryId = c.CountryId\
    	JOIN [Regulatory_Untrusted].[_Eforms].[Province] p\
    	ON c.ProvinceId = p.ProvinceId\
    ) as oas\
    ON core.LivelinkCompoundDocumentId = oas.FilingId WHERE oas.FilingId IS NOT NULL \
    ORDER BY core.IssuedDate Desc , core.DecisionItemId Desc'
   

#q = '''SELECT * FROM [B-06803].[OASImportExportContact] WHERE FormId = ? '''
#cursor.execute(q,4827)
#row = cursor.fetchall()
#df = pd.DataFrame(zip(row))
    
def view():
    df1 = pd.read_sql(query,conn)
    df1.sort_values(by=['IssuedDate','DecisionItemId'], ascending = False)
    df1 = df1[0:40]
    return df1

def form_contacts(fid):
    cursor.execute(q,fid)
    row = cursor.fetchall()
    df = pd.DataFrame(row)
    return df


def view_gener(conn, q):
    df1 = pd.read_sql(q,conn)
    df1.sort_values(by=['ReceivedDate','DecisionItemId'], ascending = False)
    df1 = df1[0:40]
    return df1







    