#This is a management tool to facilitate the overseeing of the outstanding SAR searches
#It will run through the specified SARs and report on all outstanding work
#OSRT ----> Outstanding Searches Reporting Tool

###############################################################################################################################

## import all the necessary libraries
import os,requests,multiprocessing,configparser,csv,boto3
import gspread,json,getpass,sys,time, string,itertools
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.http import MediaFileUpload
from datetime import datetime
from datetime import timedelta

################################################################################################################################
paramsPath = os.path.join(os.path.expanduser('~'),'ConfigFiles','params_olu.cfg')

#set up access to the config file
config = configparser.RawConfigParser()

#read the config
config.read(paramsPath)

gSpreadkey = config['GSpread Details']['gspread_key_file']
gSpreadKeyPath = os.path.join(os.path.expanduser('~'),'ConfigFiles',gSpreadkey)

googlekey = config['Google Drive']['google_key_file']
googleKeyPath = os.path.join(os.path.expanduser('~'),'ConfigFiles',googlekey)



#Report Folder Location ID (known)
reportFolder = config['OSRT']['reportFolderId']

#Report Template File Id (known)
templateSpreadsheetFileId = config['OSRT']['templateSpreadsheetFileId']

#The prefix for the report name
reportNamePrefix = config['OSRT']['reportNamePrefix']



# Google Services of interest that we're interested in
SCOPES = ['https://www.googleapis.com/auth/drive',                                                       
          'https://www.googleapis.com/auth/drive.file',                                                  
          'https://www.googleapis.com/auth/spreadsheets']

#GSpread Authentication setup
def authorizeGSpread(pathToGSpreadConfigFile,scopes):
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        pathToGSpreadConfigFile,SCOPES)
    gs = gspread.authorize(creds)
    return gs

########################Function definitions#####################################

# Function to connect to the Google Service API
def get_google_service(api_name, api_version, scopes, key_file_location):
    """Get a service that communicates to a Google API.
        Args:
            api_name: The name of the api to connect to.
            api_version: The api version to connect to.
            scopes: A list auth scopes to authorize for the application.
            key_file_location: The path to a valid service account JSON key file.
        Returns:
            A service that is connected to the specified Google API.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        key_file_location, scopes=scopes)
    # Build the service object.
    service = build(api_name, api_version, credentials=credentials)
    return service


def connectToWorkbookSheet(gsAuth,workbookId,sheetIndex):
    #connect to spreadsheet by spreadsheet ID
    #this function takes an authenthicated gspread handle,
    #the sheet to connect to, and the index of the particular worksheet
    wB = gsAuth.open_by_key(workbookId)
    #connect to the particular worksheet by index
    activeWorkSheet = wB.get_worksheet(sheetIndex)
    return activeWorkSheet

def copyAndRenameGDriveFile(gService,fileToCopyId,NewFileName):
    #This folder makes a copy of the specified Google drive file
    #and renames it in one go. The new copy will still reside in the
    #same location as the file that was copied
    #it takes
    #google service handle
    #id of the file to copy
    #new file name of the copied file
    #as input
    
    #owner ='data-compliance-team@data-team-ga-data.iam.gserviceaccount.com'
    #userPerm = 'formstack@formstack-ga-gdpr.iam.gserviceaccount.com'
    copied_file = {'name' : NewFileName}
    newFile = gService.files().copy(
                         fileId=fileToCopyId,
                         body=copied_file).execute()
        
    fileId = newFile.get('id')
    
    #return the file Id   
    return fileId


def getSubFolderIds(service):
    #we know the folder ids
    deleteFolderId = '1lqjW1n1B3HKzGSb6CAaauLYgxr98cbQz'
    sarsFolderId = '1GVrsEtMR9ojks7AWaXO7-rXeTvOxGzyU'
    page_token = None
    sList = []
    while True:
        #get the subfolders for the delete folder
        response = service.files().list(q="'1lqjW1n1B3HKzGSb6CAaauLYgxr98cbQz' in parents and trashed = false",
                                              spaces='drive',
                                              fields='nextPageToken, files(id, name)',
                                              pageToken=page_token).execute()
        for file in response.get('files', []):
            #if it has DSR in it        
            if ('- Open' in file.get('name')):
                sList.append(file.get('id'))
                #print('Found SubFolder: %s (%s) ' % (file.get('name'), file.get('id')))
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    
    #do the same for the SARs folder
    page_token = None
    while True:
        response = service.files().list(q="'1GVrsEtMR9ojks7AWaXO7-rXeTvOxGzyU' in parents and trashed = false",
                                              spaces='drive',
                                              fields='nextPageToken, files(id, name)',
                                              pageToken=page_token).execute()
        
        for file in response.get('files', []):            
            #if it has DSR in it
            if ('- Open' in file.get('name')):
                sList.append(file.get('id'))
                #print('Found SubFolder: %s (%s) ' % (file.get('name'), file.get('id')))
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return sList

def createReport(authGSpreadConn,newReportSheetId,idsList):
    #this function fill the newly created spreadsheet with the required info
        #it will create one column for each spreadsheet id in list

    #Get a connction to the report spreadsheet
    rpc = connectToWorkbookSheet(authGSpreadConn,newReportSheetId,0)

    col = 2
    for id in idsList:
        #print('Processing...',id)
        row = 1
        hCounter = 0    
        #connect to the summary report of the open SAR    
        oSheet = connectToWorkbookSheet(authGSpreadConn,id,0)
        #Get the whole column into a list 'just checking'
        contentList = oSheet.col_values(2)
        actionList = oSheet.col_values(3)
        newList = []
        
        n = len(contentList) - len(actionList)
        if n > 0:
            for i in range(0,n):
                actionList.append(i)
                   
        
        sAct = contentList[0][0] # Expecting a D or a S       
        #write header
        while (hCounter < 6):
            #update the worksheet header
            #print(contentList[j])
            #print(row, col, contentList[hCounter])
            rpc.update_cell(row ,col, contentList[hCounter])
            #time.sleep(0.75)
            row += 1
            hCounter += 1
        #Header has been filled out by the time we get here
        #Now we want to indicate the searches that need work
        #The template has a fixed structure so we can play off that fact
        #and create a loop that will fill only the cells with 'pendings'
        bCounter = 9 #data starts at row 8
        while bCounter  < len(contentList)+1:            
            #print(bCounter, '  ', contentList[bCounter-1])
            if contentList[bCounter-1] == 'Pending':
                #update the cell with the words 'Pending'
                #rpc.update_cell(bCounter-1, col, 'Pending')
                newList.append('Pending')
            elif 'Yes'.lower() in contentList[bCounter-1].lower():
                if sAct =='D':
                    if actionList[bCounter-1] == 'No':
                        strAction = ' (Not Deleted)'
                    elif actionList[bCounter-1] == 'Yes':
                        strAction = ' (Deleted)'
                    elif actionList[bCounter-1] == 'N/A':
                        strAction = ' (Not Applicable)'
                    else:
                        strAction = ' (tbc)'
                    #rpc.update_cell(bCounter-1, col, 'Yes ' + strAction)
                    newList.append('Yes ' + strAction)
                else:
                    #rpc.update_cell(bCounter-1, col, 'Yes')
                    newList.append('Yes')
            else:
                #rpc.update_cell(bCounter-1, col, 'No Data')
                newList.append('No Data')
                
            bCounter += 1
        
        strRange = getSpreadsheetRange(col, len(contentList))
        cell_list = rpc.range(strRange)
       
        for c ,n in zip(cell_list, newList):            
            c.value = n
            
        #update the spreadsheet cells range as a batch
        rpc.update_cells(cell_list)
        
        col += 1 #prepare for the next 'id'

        
        
def getTimeString():
    return datetime.strftime(datetime.now(),"%d/%m/%y %H:%M:%S")



def writeToLog(authSheetService,logSheetId,whatToWrite):
    '''This function writes info to the bespoke log file which is a google sheet. This was done
    because there will not be access to the standard logs on AWS
    It takes an authenticate qspread connector, the id of the log file and the what to write as input'''
    #connect to the log file
    logSheet = connectToWorkbookSheet(authSheetService,logSheetId,0)
    #get the next row number to write to
    nextRowNumber = getNextFillRow(logSheet)
    #write to the first column - hence the '1'
    logSheet.update_cell(str(nextRowNumber),'1',whatToWrite)


def getNextFillRow(sheetHandle):
    '''this function takes a worksheet handle and returns the next empty row
    all it does is count the rows filled out for column 1. Google forms will fill
    the spreasheet sequentially with no gaps. The first column in the spreadsheet is the
    system generated timestamp so no chance of the column being blank which could
    cause this function to be unpredictable'''
    return (len(sheetHandle.col_values(1)) + 1)


       

def getNewReportName(reportPrefix):
    #this function returns the name of the new report with the time created appended
        
    #constitute the name of the report using the date and time report was generated
    newFName = reportPrefix + getTimeString()
    return newFName


def getSpreadsheetIdList(service, folderIdList):
    #this function searches for all the summary spreadsheets in the folderlist
    #ideally there should only be one
    sList = []
    page_token = None
    if(len(folderIdList) == 0):
        print('Error...Empty Folder List')
        return sList
    
    for id in folderIdList:
        qStr = buildQString(id)
        response = service.files().list(q=qStr,
                                        spaces='drive',
                                        fields='nextPageToken, files(id)',
                                        pageToken=page_token).execute()
        sFile = response.get('files', [])
        if(len(sFile) > 0):
            sList.append(sFile[0]['id'])
       
    return sList

def buildQString(parentFolderId):
    #this function builds the query string for the files.list function
    #we are looking for a file that
    #has a particulat parent
    #has DSR Reference Number in its name
    #is a spreadsheet (this function will not work on older SARs)
    #has not been deleted
    #it returns a well built string ****gave me a lot of headache******
    
    qStr =  "'" + parentFolderId + "'" + " in parents"
    qStr += " and name contains "
    qStr += "'DSR Reference Number'"
    qStr += " and mimeType = 'application/vnd.google-apps.spreadsheet'"
    qStr += " and trashed = false" 
    return qStr

def getSpreadsheetRange(colIndex, lastRow):
    #function builds a list and and looks it up and returns the one for the colIndex
    aList = []
    for a in string.ascii_uppercase:
        aList.append(a)
    for a in string.ascii_uppercase:
        aList.append(aList[0] + a)
    #'we now have a string we can do a lookup from
    
    return(aList[colIndex-1] + '8:' + aList[colIndex-1] + str(lastRow))
 
        
###########################End of function definitions#############################################

#Create the connection to access spreadsheets
authGSConnection = authorizeGSpread(gSpreadKeyPath,SCOPES)

#get the connection to access google file service 
googleFileService = get_google_service(
        api_name='drive',
        api_version='v3',
        scopes=SCOPES,
        key_file_location=googleKeyPath)

#################################################################################################
#see that we can write to the log file
logFileId = config['LogFiles ID']['osrtReportingTool']
writeToLog(authGSConnection,logFileId, getTimeString() + '  Process started')


#get the name for the new report
newFName = getNewReportName(reportNamePrefix)

#get the ids of all the open SARs folders                                
folderIdsList = getSubFolderIds(googleFileService)

#get all the spreadsheet in those folders that meet our criteria
sheetIdsList = getSpreadsheetIdList(googleFileService,folderIdsList)

#find out how many
numSars = len(sheetIdsList)

#if there are open SARs that qualify
if (numSars > 0):
    writeToLog(authGSConnection,logFileId,'Creating New Report: ' + newFName)
    #create a copy of the template and rename it
    newReportSheetId = copyAndRenameGDriveFile(googleFileService,templateSpreadsheetFileId,newFName)   
    #tell user something via the log file
    writeToLog(authGSConnection,logFileId,'Open SARs Found: ' + str(numSars))
    #read the spreadsheets and fill the report
    writeToLog(authGSConnection,logFileId,'Generating Report...' )
    createReport(authGSConnection,newReportSheetId,sheetIdsList)
else:
    print('No Outstanding Searches Found')

writeToLog(authGSConnection,logFileId, getTimeString() + '  Process Ended')
writeToLog(authGSConnection,logFileId, '******************') #separator

#############################The End#######################################################




           








