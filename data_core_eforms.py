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
        #app = str()
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
        else:
            return 'this is not a gas order'
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
#            if gas_type == '1':
#                gas_en,gas_fr = str(),str()
            if gas_type == '2':
                gas_en = 'natural gas, in the form of Liquefied Natural Gas'
                gas_fr = 'gaz, sous la forme de gaz naturel liquéfié seulement'
            elif gas_type == '3':
                gas_en = 'natural gas, in the form of compressed natural gas'
                gas_fr = 'gaz, sous la forme de gaz naturel comprimé'
                
            return gas_en , gas_fr
        else:
            return 'other comms....'
        
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
def commence_end_order(ctype:str, df:pd.DataFrame) -> list:
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







    