#-------------------------------------------------------------------------------
# Name:        IMERG_ETL.py
# Purpose:     SERVIR Implementation of IMERG ETL Scripts for various ArcGIS products and services.
# Author:      Kris Stanton
# Last Modified By: Githika Tondapu Mar 30,2018
# Created:     2015
# Copyright:   (c) SERVIR 2015
# Licence:     <your licence>
#
# Note: Portions of this code may have been adapted from other code bases and authors
#-------------------------------------------------------------------------------

# Notes
# Files that are extracted for TRMM are in an expected filename format
# 'TRMM-3B42RT-V7-Rain_2014-05-03T03Z.tif.gz' as an example

import arcpy
from arcpy import env
import datetime
import time
import os
import urllib
import urllib2
import sys
import zipfile
import gzip
import shutil
import json
import ftplib
import re
import pickle
# SD's files: "arcpy_utils.py" and "etl_utls"
from arcpy_utils import FileGeoDatabase, RasterCatalog, AGServiceManager
from etl_utils import FTPDownloadManager

# SD's file for creating static maps.
from arcpy_trmm_custom_raster import TRMMCustomRasterRequest, TRMMCustomRasterCreator
from copy import deepcopy

from time import mktime
from datetime import timedelta
#from datetime import datetime

import boto    

# ETL Support Items (Used in ALL ETLs)
import ks_ConfigLoader      # Handles loading the xml config file
import ks_AdpatedLogger     # Handles logging items in a standardized way

# This is the location of the config file of which the contents are then used in script execution.
g_PathToConfigFile = r"D:\SERVIR\Scripts\IMERG\config_IMERG.xml"

# Load the Config XML File into a settings dictionary
g_ConfigSettings = ks_ConfigLoader.ks_ConfigLoader(g_PathToConfigFile)

# Detailed Logging Setting, Default to False
g_DetailedLogging_Setting = False

# Using a boundary file to clip incomming rasters for the 3 hour.
g_BoundaryFolder = r"D:\SERVIR\Scripts\IMERG\BoundaryFile"
g_BoundaryFileName = "IMERGBoundary.shp"

# Loads the Settings object.
def get_Settings_Obj():
    Current_Config_Object = g_ConfigSettings.xmldict['ConfigObjectCollection']['ConfigObject']
    return Current_Config_Object

# Needed to prevent errors (while the 'printMsg' function is global...)
settingsObj = get_Settings_Obj()
# Logger Settings Vars
theLoggerOutputBasePath = settingsObj['Logger_Output_Location']
theLoggerPrefixVar = settingsObj['Logger_Prefix_Variable']
theLoggerNumOfDaysToStore = settingsObj['Logger_Num_Of_Days_To_Keep_Log'] 
# KS Mod, 2014-01   Adding a Script Logger 3        START
g_theLogger = ks_AdpatedLogger.ETLDebugLogger(theLoggerOutputBasePath, theLoggerPrefixVar+"_log", {

        "debug_log_archive_days":theLoggerNumOfDaysToStore
    })


# Add to the log
def addToLog(theMsg, detailedLoggingItem = False):

    global g_theLogger, g_DetailedLogging_Setting
    if detailedLoggingItem == True:
        if g_DetailedLogging_Setting == True:
            # This configuration means we should record detailed log items.. so do nothing
            pass
        else:
            # This config means we should NOT record detailed log items but one was passed in, so using 'return' to skip logging
            return

    # These lines wrap each log entry onto a new line prefixed by the date/time of code execution
    currText = ""
    currText += theMsg
    g_theLogger.updateDebugLog(currText)

# Calculate and return time elapsed since input time
def timeElapsed(timeS):
    seconds = time.time() - timeS
    hours = seconds // 3600
    seconds -= 3600*hours
    minutes = seconds // 60
    seconds -= 60*minutes
    if hours == 0 and minutes == 0:
        return "%02d seconds" % (seconds)
    if hours == 0:
        return "%02d:%02d seconds" % (minutes, seconds)
    return "%02d:%02d:%02d seconds" % (hours, minutes, seconds)

# Get a new time object
def get_NewStart_Time():
    timeStart = time.time()
    return timeStart

# Get the amount of time elapsed from the input time.
def get_Elapsed_Time_As_String(timeInput):
    return timeElapsed(timeInput)


# Parse "0" or "1" from settings into a bool.
def get_BoolSetting(theSetting):
    try:
        if theSetting == "1":
            return True
        else:
            return False
    except:
        addToLog("get_BoolSetting: SCRIPT ERROR!! ERROR PARSING BOOL SETTING FOR (theSetting), " + str(theSetting) + ", Returning False")
        return False


# Release Candidate Function for implementation
# Force item to be in a list
def convert_Obj_To_List(item_Object):
    retList = list()

    # Quick test to see if the item is already a list
    testList = []
    isAlreadyList = False
    try:
        testList + item_Object
        isAlreadyList = True
    except:
        isAlreadyList = False

    # if the item is already a list, return it, if not, add it to an empty one.
    if isAlreadyList == True:
        return item_Object
    else:
        retList.append(item_Object)
        return retList


# Makes a directory on the filesystem if it does not already exist.
# Then checks to see if the folder exists.
# Returns True if the folder exists, returns False if it does not
def make_And_Validate_Folder(thePath):
    try:
        # Create a location for the file if it does not exist..
        if not os.path.exists(thePath):
            os.makedirs(thePath)
        # Return the status
        return os.path.exists(thePath)
    except:
        e = sys.exc_info()[0]
        addToLog("make_And_Validate_Folder: ERROR, Could not create folder at location: " + str(thePath) + " , ERROR MESSAGE: "+ str(e))
        return False

# returns todays date minus the interval ("90 days") for example
def Unsorted_GetOldestDate(intervalString):
    try:
        intervalValue = int(intervalString.split(" ")[0])
        intervalType = intervalString.split(" ")[1]

        deltaArgs = {intervalType:intervalValue}
        # Get the oldest date before now based on the interval and date format
        oldestDate = datetime.datetime.utcnow() - datetime.timedelta(days=intervalValue) #datetime.timedelta(**deltaArgs)
    except:
        e = sys.exc_info()[0]
        print("    Error getting oldest date: System Error message: "+ str(e))
        return None

    return oldestDate


# KS Refactor For 30 Min Datasets // Need to clean up the GeoDB of any 3Hr datasets (this is because we are using the same raster mosaic dataset to hold the 30 min items) // This needs to be removed once we have a 3 hour dataset
def Unsorted_Clean_GeoDB_Of_Old_3Hr_Datasets__30MinRefactorCleanupFunction(varList,mdWS,oldDate,qryDateFmt):
    try:
        numRemoved = 0
        for varDict in varList:
            mosaicDSName = varDict["mosaic_name"]
            dateField = varDict["primary_date_field"]
            mosaicDS = os.path.join(mdWS, mosaicDSName)
            query = "Name LIKE '%.3hr'"  # STATE_NAME LIKE 'Miss%'
            addToLog("Unsorted_Clean_GeoDB_Of_Old_3Hr_Datasets__30MinRefactorCleanupFunction: query "+str(query), True)

            try:
                # Remove the rasters from the mosaic dataset based on the query
                startCount = int(arcpy.GetCount_management(mosaicDS).getOutput(0))
                arcpy.RemoveRastersFromMosaicDataset_management(mosaicDS, str(query), "NO_BOUNDARY", "NO_MARK_OVERVIEW_ITEMS", \
                                                                "NO_DELETE_OVERVIEW_IMAGES", "NO_DELETE_ITEM_CACHE", \
                                                                "REMOVE_MOSAICDATASET_ITEMS", "NO_CELL_SIZES")
                endCount = int(arcpy.GetCount_management(mosaicDS).getOutput(0))
                addToLog("Unsorted_Clean_GeoDB_Of_Old_3Hr_Datasets__30MinRefactorCleanupFunction: Removed "+str(startCount-endCount)+" rasters ("+str(query)+") from "+str(mosaicDSName))
                numRemoved = numRemoved + (startCount-endCount)
            # Handle errors for removing rasters
            except:
                addToLog("Unsorted_Clean_GeoDB_Of_Old_3Hr_Datasets__30MinRefactorCleanupFunction: Error removing rasters from "+mosaicDSName+", ArcPy message"+str(arcpy.GetMessages()))

    except:
        e = sys.exc_info()[0]
        addToLog("Unsorted_Clean_GeoDB_Of_Old_3Hr_Datasets__30MinRefactorCleanupFunction: ERROR, Something went Wrong trying to clean old 3hour items from the geodb: System Error message: " + str(e))

    addToLog("Unsorted_Clean_GeoDB_Of_Old_3Hr_Datasets__30MinRefactorCleanupFunction: Reached the End!")


# Remove old raster(s) from the mosaic dataset(s) and remove the files from
#   the file system if they get removed from the mosaic dataset
#   Return the number of rasters removed
def Unsorted_removeRastersMosaicDataset(varList,mdWS,oldDate,qryDateFmt):
    numRemoved = 0
    for varDict in varList:
        mosaicDSName = varDict["mosaic_name"]
        dateField = varDict["primary_date_field"]
        mosaicDS = os.path.join(mdWS, mosaicDSName)

        if not dateField:
            addToLog("Unsorted_removeRastersMosaicDataset: No primary date field defined for "+mosaicDSName+".  No rasters removed")
            pass
        else:
            dstr = oldDate.strftime(qryDateFmt)
            query = dateField + " < date '" + dstr + "'"

            addToLog("Unsorted_removeRastersMosaicDataset: query "+str(query), True)

            try:
                # Remove the rasters from the mosaic dataset based on the query
                startCount = int(arcpy.GetCount_management(mosaicDS).getOutput(0))
                arcpy.RemoveRastersFromMosaicDataset_management(mosaicDS, str(query), "NO_BOUNDARY", "NO_MARK_OVERVIEW_ITEMS", \
                                                                "NO_DELETE_OVERVIEW_IMAGES", "NO_DELETE_ITEM_CACHE", \
                                                                "REMOVE_MOSAICDATASET_ITEMS", "NO_CELL_SIZES")
                endCount = int(arcpy.GetCount_management(mosaicDS).getOutput(0))

                addToLog("Unsorted_removeRastersMosaicDataset: Removed "+str(startCount-endCount)+" rasters ("+str(query)+") from "+str(mosaicDSName))
                numRemoved = numRemoved + (startCount-endCount)

            # Handle errors for removing rasters
            except:

                addToLog("Unsorted_removeRastersMosaicDataset: Error removing rasters from "+mosaicDSName+", ArcPy message"+str(arcpy.GetMessages()))
                pass

    # KS Refactor For 30 Min Datasets // # Clean up old 3hr items from the geodb
    Unsorted_Clean_GeoDB_Of_Old_3Hr_Datasets__30MinRefactorCleanupFunction(varList,mdWS,oldDate,qryDateFmt)
    return numRemoved


# Cleans up old files from the output raster location (file system)
def Unsorted_dataCleanup(rasterOutputLocation,oldDate, regExp_Pattern, rastDateFormat): #,dateFmt):
    numDeleted = 0
    arcpy.env.workspace = rasterOutputLocation
    dateFmt = "%Y%m%d%H"
    oldDateStr = oldDate.strftime(dateFmt)
    oldDateInt = int(oldDateStr)
    dateFmt_imerg = "%Y%m%d"
    oldDateStr_imerg = oldDate.strftime(dateFmt_imerg)
    oldDateInt_imerg = int(oldDateStr_imerg)

    addToLog("dataCleanup: Deleting rasters older than, "+str(oldDateInt_imerg))

    # Now override some of the inputs
    regExp_Pattern = "\\d{4}[01]\\d[0-3]\\d"
    rastDateFormat = "%Y%m%d" # which works for imerg instead of %Y%m%d%H, which worked for TRMM
    dateFmt = dateFmt_imerg

    try:
        for raster in arcpy.ListRasters("*", "All"):
            rasterDatesFoundList = re.findall(regExp_Pattern,str(raster))
            rastDateStr = rasterDatesFoundList[0]
            tempDateTime = datetime.datetime.strptime(rastDateStr, rastDateFormat)
            tempDateTimeStr = tempDateTime.strftime(dateFmt)
            rastDateInt = int(tempDateTimeStr)
            # KS Refactor..  if a delete operation fails, the code keeps on going and tries the next one....
            try:
                if(oldDateInt_imerg > rastDateInt):
                    arcpy.Delete_management(raster)
                    addToLog ("dataCleanup: Deleted "+raster,True)
                    numDeleted = numDeleted + 1
                else:
                    pass
            except:
                addToLog("dataCleanup: Error Deleting "+raster+" ArcPy Message: "+str(arcpy.GetMessages()))
    # Handle errors for deleting old raster files
    except:
        addToLog("dataCleanup: Error cleaning up old raster files from "+rasterOutputLocation+" ArcPy Message: "+str(arcpy.GetMessages()))

    return numDeleted

#--------------------------------------------------------------------------
# Pre ETL
#--------------------------------------------------------------------------

# Converts XML read var dictionary settings into a standard "VarDictionary" object
#  Sometimes "ListItem" and "service_dict_list" only contain one element.  When that happens, their types need to be converted to lists.
#  This method handles that conversion.
def PreETL_Support_Get_Standard_VarDictionary_From_RawVarSettings(RawVarSettings):
    # Force the entire item to be a list
    varSettings_1 = convert_Obj_To_List(RawVarSettings)

    # The root level entry, called, "ListItem" also needs to be forced into a list
    listItem_List = convert_Obj_To_List(varSettings_1[0]['ListItem'])

    # For each list item, we need to make sure that the child element, 'service_dict_list' is ALSO a list.
    rebuilt_ListItem_List = list()
    for currListItem in listItem_List:
        currListItem['service_dict_list'] = convert_Obj_To_List(currListItem['service_dict_list'])
        rebuilt_ListItem_List.append(currListItem)

    # Now rebuild the Return object
    retVarDict = rebuilt_ListItem_List

    # Return the result
    return retVarDict

# Validate Config, Create Workspaces
def PreETL_Support_CreateWorkspaceFolders(theScratchWorkspace_BasePath):
    # Assemble the input folder paths to create.
    workSpacePath_PreETL = theScratchWorkspace_BasePath + "\\PreETL"
    workSpacePath_Extract = theScratchWorkspace_BasePath + "\\Extract"
    workSpacePath_Transform = theScratchWorkspace_BasePath + "\\Transform"
    workSpacePath_Load = theScratchWorkspace_BasePath + "\\Load"
    workSpacePath_PostETL = theScratchWorkspace_BasePath + "\\PostETL"

    # Create the folders and set the flag if any fail.
    foldersExist = True
    checkList = list()
    checkList.append(make_And_Validate_Folder(workSpacePath_PreETL))
    checkList.append(make_And_Validate_Folder(workSpacePath_Extract))
    checkList.append(make_And_Validate_Folder(workSpacePath_Transform))
    checkList.append(make_And_Validate_Folder(workSpacePath_Load))
    checkList.append(make_And_Validate_Folder(workSpacePath_PostETL))
    if False in checkList:
        foldersExist = False

    # package up the return object
    retObj = {
        "PreETL":workSpacePath_PreETL,
        "Extract":workSpacePath_Extract,
        "Transform":workSpacePath_Transform,
        "Load":workSpacePath_Load,
        "PostETL":workSpacePath_PostETL,
        "FoldersExist": foldersExist
    }

    return retObj

# Returns True if the workspace path and type are valid, Returns False if not valid or on error.
def PreETL_Support_Validate_Dataset_Workspace(theWorkspacePath):
    try:
        if not arcpy.Exists(theWorkspacePath):
            addToLog("PreETL_Support_Validate_Dataset_Workspace: Error: Workspace path, "+str(theWorkspacePath)+", does not exist")
            return False
        else:
            addToLog("PreETL_Support_Validate_Dataset_Workspace: about to arcpy.Describe the workspace path, "+str(theWorkspacePath), True)
            descWS = arcpy.Describe(theWorkspacePath)
            if not descWS.dataType == "Workspace":
                addToLog("PreETL_Support_Validate_Dataset_Workspace: Error: The Workspace must be of datatype 'Workspace'.  The current datatype is: "+str(descWS.dataType))
                return False
            else:
                return True
    except:
        e = sys.exc_info()[0]
        addToLog("PreETL_Support_Validate_Dataset_Workspace: ERROR, something went wrong, ERROR MESSAGE: "+ str(e))
        return False
    return False

# Returns True if the output raster directory exists or gets created.  Returns False on error
def PreETL_Support_Create_RasterOutput_Location(theRasterOutputPath):
    return make_And_Validate_Folder(theRasterOutputPath)


# This function would be called by the main controller and would either just execute some simple process, or call on the support method(s) immediately above to execute a slightly more complex process.
def PreETL_Controller_Method(ETL_TransportObject):

    # Any other PreETL procedures could go here...

    # Make the Variable Dictionary Object
    addToLog("PreETL_Controller_Method: Validating Variable_Dictionary_List", True)
    Variable_Dictionary_List = PreETL_Support_Get_Standard_VarDictionary_From_RawVarSettings(ETL_TransportObject['SettingsObj']['VariableDictionaryList'])

    # Validate Config - Create Workspace folders
    addToLog("PreETL_Controller_Method: Validating Scratch_WorkSpace_Locations", True)
    Scratch_WorkSpace_Locations = PreETL_Support_CreateWorkspaceFolders(ETL_TransportObject['SettingsObj']['ScratchFolder'])

    # Validate Config - Make sure the data set work space exists (Path to GeoDB or SDE connection)
    addToLog("PreETL_Controller_Method: Joining Folders to create GeoDB_Dataset_Workspace", True)
    GeoDB_Dataset_Workspace = os.path.join(ETL_TransportObject['SettingsObj']['GeoDB_Location'], ETL_TransportObject['SettingsObj']['GeoDB_FileName'])
    addToLog("PreETL_Controller_Method: Validating GeoDB_Dataset_Workspace", True)
    is_Dataset_Workspace_Valid = PreETL_Support_Validate_Dataset_Workspace(GeoDB_Dataset_Workspace)

    # Validate Config - Make sure the output Raster Directory exists.
    RasterOutput_Location = ETL_TransportObject['SettingsObj']['Raster_Final_Output_Location']
    is_RasterOutLocation_Valid = PreETL_Support_Create_RasterOutput_Location(RasterOutput_Location)

    # Any other PreETL procedures could also go here...


    # Check the above setup for errors
    IsError = False
    ErrorMessage = ""

    # Validate - Checking if scratch workspace folders were created
    if Scratch_WorkSpace_Locations['FoldersExist'] == False:
        IsError = True
        ErrorMessage += "ERROR: One of the scratch workspace folders was unable to be created.  | "

    # Validate - Checking if workspace is valid
    if is_Dataset_Workspace_Valid == False:
        IsError = True
        ErrorMessage += "ERROR: The arc workspace either does not exist or is of an invalid type.  | "

    # Validate - Make sure raster output path exists or was created
    if is_RasterOutLocation_Valid == False:
        IsError = True
        ErrorMessage += "ERROR: The raster output location, " + str(RasterOutput_Location) + ", does not exist or was unable to be created."


    # Package up items from the PreETL Step
    returnObj = {
        'Variable_Dictionary_List': Variable_Dictionary_List,
        'Scratch_WorkSpace_Locations': Scratch_WorkSpace_Locations,
        'GeoDB_Dataset_Workspace':GeoDB_Dataset_Workspace,
        'RasterOutput_Location':RasterOutput_Location,

        'IsError': IsError,
        'ErrorMessage':ErrorMessage
    }

    # Return the packaged items.
    return returnObj



#--------------------------------------------------------------------------
# Extract
#--------------------------------------------------------------------------

def Extract_Support_GetStartDate(primaryDateField, mosaicDS):
    arcpy.RemoveRastersFromMosaicDataset_management(in_mosaic_dataset="E:/SERVIR/Data/Global/IMERG_SR3857.gdb/IMERG", where_clause="timestamp IS NULL or Name is NULL or Raster IS NULL", update_boundary="UPDATE_BOUNDARY", mark_overviews_items="MARK_OVERVIEW_ITEMS", delete_overview_images="DELETE_OVERVIEW_IMAGES", delete_item_cache="DELETE_ITEM_CACHE", remove_items="REMOVE_MOSAICDATASET_ITEMS", update_cellsize_ranges="UPDATE_CELL_SIZES")
    startDate = None
    try:
        # KS Refactor for Early Data // Compare the Name of the file, only add dates that have 'L' for "Late" in their name.
        rasterNameField = 'Name'
        theFields = [primaryDateField, rasterNameField]
        dateList = []
        for row in arcpy.da.SearchCursor(mosaicDS,theFields):
            currentDate = row[0]
            currentName = row[1]
            # We only want a list of 'Late" datasets, this is how to tell
            # Sample filename for 'Late':      3B-HHR-L.MS.MRG.3IMERG.20150802-S083000-E085959.0510.V03E.30min
            if currentName[7] == 'L':
                varx = str(currentDate)
                dateList.append(varx)
        sortedDates = sorted(dateList)

    except:
        
        print "in f except"
        e = sys.exc_info()[0]
        addToLog("exception"+str(e))
    try:
        maxDate = sortedDates[-1]
        #dt = datetime.fromtimestamp(mktime(maxDate))

        #if (not startDate) or (dt < startDate):        
        startDate = maxDate
        print startDate
			
    except:
        startDate = datetime.datetime.now()+datetime.timedelta(-90)# datetime.timedelta(-30) #maxDate

    if startDate == None:
        startDate =datetime.datetime.now()+datetime.timedelta(-90)
    return startDate


# Simillar to the function Extract_Support_Get_PyDateTime_From_String, but returns only the string component.
def Extract_Support_Get_DateString_From_String(theString, regExp_Pattern):
    try:
        # Search the string for the datetime format
        reItemsList = re.findall(regExp_Pattern,theString)
        if len(reItemsList) == 0:
            # No items found using the Regular expression search
            return None
        else:
            return reItemsList[0]
    except:
        return None

# Search a string (or filename) for a date by using the regular expression pattern string passed in,
# Then use the date format string to convert the regular expression search output into a datetime.
# Return None if any step fails.
def Extract_Support_Get_PyDateTime_From_String(theString, regExp_Pattern, date_Format):
    try:
        # Search the string for the datetime format
        reItemsList = re.findall(regExp_Pattern,theString)
        if len(reItemsList) == 0:
            # No items found using the Regular expression search
            # If needed, this is where to insert a log entry or other notification that no date was found.
            return None
        else:
            retDateTime = datetime.strptime(reItemsList[0], date_Format)
            return retDateTime
    except:
        return None



# Support Method which returns a list of files that fall within the passed in date range.
def Extract_Support_GetList_Within_DateRange(the_ListOf_AllFiles, the_FileExtn, the_Start_DateTime, the_End_DateTime, regExp_Pattern, date_Format):
    retList = []
    list_Of_FileNames = []
    if the_FileExtn:
        list_Of_FileNames = [f.split(" ")[-1] for f in the_ListOf_AllFiles if f.endswith(the_FileExtn)]
    else:
        list_Of_FileNames = [f.split(" ")[-1] for f in the_ListOf_AllFiles]

    # Now iterate through the list and only add the ones that match the critera
    for currFileName in the_ListOf_AllFiles:
        currFileNameDateTime = Extract_Support_Get_PyDateTime_From_String(currFileName, regExp_Pattern, date_Format)
        try:
            if ((currFileNameDateTime > the_Start_DateTime) and (currFileNameDateTime <= the_End_DateTime)):
                retList.append(currFileName)
        except:
            # String probably was "None" type, try the next one!
            pass

    return retList


# KS Refactor For 30 Min Datasets
# Rebuild the time coded part of the file name based on the hour and minute values.. they are predictable!
# this function needs to build a string that looks something like this "-S030000-E032959.0180"
def get_IMERG_S_String_From_Hour_And_Minute(theHour,theMinute):
    retString = ""
    # Append the first part (beginning of the 'start time' part)
    retString += "-S"

    # Append the hour part
    if theHour < 10:
        retString += "0"
    retString += str(theHour)

    # Append the minute part
    if theMinute < 10:
        retString += "0"
    retString += str(theMinute)

    # Append the Seconds and beginning of the 'end time' part
    retString += "00-E"

    # Append the hour part (same as before)
    if theHour < 10:
        retString += "0"
    retString += str(theHour)

    # Append the 'end minute' part
    if theMinute == 0:
        retString += "29"
    else:
        retString += "59"

    # Append the 'end seconds' part and the dot that comes before the 'minutes code'
    retString += "59."

    # Append the 'minutes code' # these fun if statements are to account for the fact that this code MUST be 4 characters
    minuteCodeValue = (theHour * 60) + theMinute
    if minuteCodeValue < 1000:
        retString += "0"
    if minuteCodeValue < 100:
        retString += "0"
    if minuteCodeValue < 10:
        retString += "0"
    retString += str(minuteCodeValue)
    # finally.. return!
    return retString


# KS Refactor For 30 Min Datasets
# 30 Minute version of Extract_Support_Get_Expected_FTP_Paths_From_DateRange
# 30 min file example:      3B-HHR-L.MS.MRG.3IMERG.20150701-S003000-E005959.0030.V03E.30min.tif
# 3hr file example:         3B-HHR-L.MS.MRG.3IMERG.20150401-S010000-E012959.0060.V03E.3hr.tfw
def Extract_Support_Get_Expected_FTP_Paths_From_DateRange_For_30Min_Datasets(start_DateTime, end_DateTime, root_FTP_Path, the_FTP_SubFolderPath):
    retList = []
    the_DateFormatString = "%Y%m%d%H" # When appending the string below, the hour component needs to be chopped off
    the_DateFormatString_WithMinutes = "%Y%m%d%H%M"
    the_DateFormatString_ForFileName = "%Y%m%d"
    the_FileNamePart1 = "3B-HHR-L.MS.MRG.3IMERG."        # IMERG Product ID?
    the_FileNameEnd_30Min_Base = ".V03E.30min" # Version and time frame   # renamed var 'the_FileNameEnd_3Hr_Base' to 'the_FileNameEnd_30Min_Base'
    the_FileNameEnd_Tif_Ext = ".tif"     # Tif file
    the_FileNameEnd_Tfw_Ext = ".tfw"     # World File
    currentDateTime = start_DateTime

    while currentDateTime < end_DateTime:


        # Do processing
        # Build all the object props based on currentDateTime and filenames etc.. BUILD the folder paths
        currentDateTime = datetime.datetime.strptime(currentDateTime,"%Y%m%d%H%M%S")

        currentDateString = currentDateTime.strftime(the_DateFormatString)
        currentDateString_WithMinutes = currentDateTime.strftime(the_DateFormatString_WithMinutes)
        currentYearString = currentDateTime.strftime("%Y")

        currentMonthString = currentDateTime.strftime("%m")
        currentDateString_ForFileName = currentDateTime.strftime(the_DateFormatString_ForFileName)
        # Needed for IMERG Refactor
        currentHourString = currentDateTime.strftime("%H")
        currentMinuteString = currentDateTime.strftime("%M")
        current_imerg_S_String = get_IMERG_S_String_From_Hour_And_Minute(int(currentHourString), int(currentMinuteString))
        currentRasterBaseName = the_FileNamePart1 + currentDateString_ForFileName + current_imerg_S_String + the_FileNameEnd_30Min_Base
        if int(currentYearString)==datetime.datetime.now().year:
			currentFTP_Subfolder = the_FTP_SubFolderPath + "/" + currentMonthString # IMERG TIF FTP has a different folder structure.. only the months..
			currentFTPFolder = root_FTP_Path + "/" + currentMonthString # IMERG TIF FTP has a different folder structure.. only the months..

        else:
			currentFTP_Subfolder = the_FTP_SubFolderPath + "/" + currentYearString+"/"+currentMonthString # IMERG TIF FTP has a different folder structure.. only the months..
			currentFTPFolder = root_FTP_Path + "/" +currentYearString+"/"+ currentMonthString # IMERG TIF FTP has a different folder structure.. only the months..
      

        current_3Hr_Tif_Filename = currentRasterBaseName + the_FileNameEnd_Tif_Ext
        current_3Hr_Twf_Filename = currentRasterBaseName + the_FileNameEnd_Tfw_Ext
        currentPathToTif = currentFTPFolder + "/" + current_3Hr_Tif_Filename
        currentPathToTwf = currentFTPFolder + "/" + current_3Hr_Twf_Filename

        # Load object
        # Create an object loaded with all the params listed above
        currentObj = {
            "FTPFolderPath" : currentFTPFolder,              
            "FTPSubFolderPath" : currentFTP_Subfolder,             
            "BaseRasterName" : currentRasterBaseName,           
            "FTP_PathTo_TIF" : currentPathToTif,               
            "FTP_PathTo_TFW" : currentPathToTwf,               
            "TIF_30Min_FileName" : current_3Hr_Tif_Filename,     
            "TWF_30Min_FileName" : current_3Hr_Twf_Filename,       
            "DateString" : currentDateString,
            "DateString_WithMinutes" : currentDateString_WithMinutes
        }

        # Add object to list
        # Add the object to the return list.
        retList.append(currentObj)
        # Incremenet to next currentDateTime
        currentDateTime= currentDateTime + datetime.timedelta(minutes=30)
        #newDateTimeString = datetime.datetime.strptime(theDateTime,"%Y-%m-%d %H:%M:%S")
        currentDateTime=currentDateTime.strftime("%Y%m%d%H%M%S")
    return retList


# KS Refactor For 30 Min Datasets
# Returns a date time which has a new minute value (Meant for standardizing the minutes to 30 minute increments)
def Extract_Support_Set_DateToStandard_30_Minute(minuteValue, theDateTime):
    formatString = "%Y%m%d%H%M"
    newDateTimeString = datetime.datetime.strptime(theDateTime,"%Y-%m-%d %H:%M:%S")
    newDateTimeString=newDateTimeString.strftime("%Y%m%d%H")

    if minuteValue < 10:
        newDateTimeString += "0"
    newDateTimeString += str(minuteValue)
    newDateTime = datetime.datetime.strptime(newDateTimeString,"%Y%m%d%H%M%S")
    newDateTime = newDateTime.strftime(formatString)
    return newDateTime


# KS Refactor For 30 Min Datasets
# Get last 30 min value from current
def Extract_Support_Get_Last_30_Min(currentMin):
    minToReturn = None
    if currentMin < 30:
        minToReturn = 0
    else:
        minToReturn = 30
    return minToReturn

# Get next 3 hour value from current hour.
def Extract_Support_Get_Next_3_Hour(currentHour):
    hourToReturn = None
    if currentHour % 3 == 0:
        hourToReturn = currentHour
    elif currentHour % 3 == 1:
        hourToReturn = currentHour + 2
    else:
        hourToReturn = currentHour + 1

    if hourToReturn > 21:
        hourToReturn = 21

    return hourToReturn


def Extract_FTP(dateFormat_String, startDateTime_str, endDateTime_str, theExtractWorkspace):

    addToLog("Extract_FTP: Started") # , True)
    pkl_file = open('config.pkl', 'rb')
    myConfig = pickle.load(pkl_file)
    pkl_file.close()
    # Move these to settings at the earliest opportunity!!
    # IMERG Refactor, new ftp path is ftp://jsimpson.pps.eosdis.nasa.gov/data/imerg/gis/04/
	Data_Directory = "https://proxy.servirglobal.net/ProxyFTP.aspx?url=ftp://jsimpson.pps.eosdis.nasa.gov/data/imerg/gis/"     
	the_FTP_Host = myConfig['ftp_host'] #"trmmopen.gsfc.nasa.gov" #"198.118.195.58" #trmmopen.gsfc.nasa.gov"  #"ftp://trmmopen.gsfc.nasa.gov"
    the_FTP_SubFolderPath = myConfig['ftp_subfolder'] #"pub/gis"
    the_FTP_UserName = myConfig['ftp_user'] # "anonymous" #
    the_FTP_UserPass = myConfig['ftp_pswrd'] # "anonymous" #"anonymous" #
    root_FTP_Path = "ftp://" + str(the_FTP_Host) + "/" + the_FTP_SubFolderPath
    ExtractList = []
    lastBaseRaster = ""
    lastFTPFolder = ""
    counter_FilesDownloaded = 0
    counter_FilesExtracted = 0
    debugFileDownloadLimiter = 5000
	
    # Get the Start / End datetimes
    print str(startDateTime_str)

    startDateTime = startDateTime_str
    endDateTime = endDateTime_str

    addToLog("Extract_FTP: dateFormat_String: " + str(dateFormat_String))
    addToLog("Extract_FTP: startDateTime: " + str(startDateTime))
    addToLog("Extract_FTP: endDateTime: " + str(endDateTime))
    x=int(str(startDateTime)[14:16])
    y=int(str(endDateTime)[14:16])

    # KS Refactor For 30 Min Datasets (These next two lines work just fine for the 3 hour dataset... replacing them with a function that adjusts for the next 30 min increment)
    # Start Date adjustment
    newStart_Minute = Extract_Support_Get_Last_30_Min(x)
    print str(newStart_Minute)
    standardized_StartDate = Extract_Support_Set_DateToStandard_30_Minute(newStart_Minute, str(startDateTime))
    # KS Refactor For 30 Min Datasets (These next two lines work just fine for the 3 hour dataset... replacing them with a function that adjusts for the next 30 min increment)
    # End Date adjustment
    newEnd_Minute = Extract_Support_Get_Last_30_Min(y)
    standardized_EndDate = Extract_Support_Set_DateToStandard_30_Minute(newEnd_Minute, str(endDateTime))
    # KS Refactor For 30 Min Datasets // Created a couple of new functions including 'Extract_Support_Get_Expected_FTP_Paths_From_DateRange_For_30Min_Datasets' to handle 30 min files
    # Extract_Support_Get_Expected_FTP_Paths_From_DateRange_For_30Min_Datasets
    # get a list of all the files within the start and end date
    expected_FilePath_Objects_To_Extract_WithinRange = Extract_Support_Get_Expected_FTP_Paths_From_DateRange_For_30Min_Datasets(standardized_StartDate, standardized_EndDate, root_FTP_Path, the_FTP_SubFolderPath)
    #addToLog("Extract_FTP: expected_FilePath_Objects_To_Extract_WithinRange (list to process) " + str(expected_FilePath_Objects_To_Extract_WithinRange) , True)
    # KS Refactor for Early Data // Storying the Error Rasters in the return object
    errorRasters_List = []

    numFound = len(expected_FilePath_Objects_To_Extract_WithinRange)
    if numFound == 0:

        if startDateTime_str == endDateTime_str:
            addToLog("Extract_FTP: ERROR: No files found for the date string "+startDateTime_str)
        else:
            addToLog("Extract_FTP: ERROR: No files found between "+startDateTime_str+" and "+endDateTime_str)
    else:

        # Connect to FTP Server
        try:

            # QUICK REFACTOR NOTE: Something very strange was happening with the FTP and there isn't time to debug this issue.. going with URL Download instead for now.
            #addToLog("Extract_FTP: Connecting to FTP", True)
            ftp_Connection = ftplib.FTP(the_FTP_Host,the_FTP_UserName,the_FTP_UserPass)
            time.sleep(1)

            addToLog("Extract_FTP: Downloading TIF and TFW files for each raster", True)

            # Holding information for the last FTP folder we changed to.
            lastFolder = ""

            # Iterate through each key file path and and perform the extraction.
            for curr_FilePath_Object in expected_FilePath_Objects_To_Extract_WithinRange:

                if counter_FilesDownloaded < debugFileDownloadLimiter:
                    # FTP, Change to folder,
                    currFTPFolder = curr_FilePath_Object['FTPSubFolderPath']

                    # Only change folders if we need to.
                    if currFTPFolder == lastFolder:
                        # Do nothing

                        pass
                    else:

                        time.sleep(1)
                        addToLog("Extract_FTP: FTP, Changing folder to : " + str(currFTPFolder))
                        ftp_Connection.cwd("/" + currFTPFolder)

                        time.sleep(1)

                    lastFolder = currFTPFolder
                    try:
                        # Attempt to download the TIF and World File (Tfw)
                        Tif_file_to_download = curr_FilePath_Object['TIF_30Min_FileName'] #['TIF_3Hr_FileName']     # KS Refactor For 30 Min Datasets // Previous Rename affected this line						
                        try:
                            #addToLog("TIF file downloaded: "+Tif_file_to_download) 

                            downloadedFile_TIF = os.path.join(theExtractWorkspace,Tif_file_to_download)
                            with open(downloadedFile_TIF, "wb") as f:			
							
								ftp_Connection.retrbinary("RETR %s" % Tif_file_to_download, f.write)
								time.sleep(1)
                        except:
                            os.remove(os.path.join(theExtractWorkspace,Tif_file_to_download))
                            Tif_file_to_download = Tif_file_to_download.replace("03E", "04A")
                            downloadedFile_TIF = os.path.join(theExtractWorkspace,Tif_file_to_download)	
							
                            try:							
								with open(downloadedFile_TIF, "wb") as f:			
									ftp_Connection.retrbinary("RETR %s" % Tif_file_to_download, f.write)
									time.sleep(1)
                            except:
								os.remove(os.path.join(theExtractWorkspace,Tif_file_to_download))
								Tif_file_to_download = Tif_file_to_download.replace("04A", "04B")
								downloadedFile_TIF = os.path.join(theExtractWorkspace,Tif_file_to_download)
								try:							
									with open(downloadedFile_TIF, "wb") as f:			
										ftp_Connection.retrbinary("RETR %s" % Tif_file_to_download, f.write)
										time.sleep(1)	
								except:
									os.remove(os.path.join(theExtractWorkspace,Tif_file_to_download))
									Tif_file_to_download = Tif_file_to_download.replace("04B", "05B")
									downloadedFile_TIF = os.path.join(theExtractWorkspace,Tif_file_to_download)
									try:							
										with open(downloadedFile_TIF, "wb") as f:			
											ftp_Connection.retrbinary("RETR %s" % Tif_file_to_download, f.write)
											time.sleep(1)	
									except:
										addToLog("",True)												
						
                        Twf_file_to_download = curr_FilePath_Object['TWF_30Min_FileName'] #['TWF_3Hr_FileName']    # KS Refactor For 30 Min Datasets // Previous Rename affected this line
                        try:
                            #addToLog("TFW file downloaded: "+Tif_file_to_download) 

                            downloadedFile_TFW = os.path.join(theExtractWorkspace,Twf_file_to_download)
                            with open(downloadedFile_TFW, "wb") as f:								
								ftp_Connection.retrbinary("RETR %s" % Twf_file_to_download, f.write)
								time.sleep(1)
                        except:			
                            os.remove(os.path.join(theExtractWorkspace,Twf_file_to_download))						
                            Twf_file_to_download = Twf_file_to_download.replace("03E", "04A")
                            downloadedFile_TFW = os.path.join(theExtractWorkspace,Twf_file_to_download)
                            try:														
								with open(downloadedFile_TFW, "wb") as f:								
									ftp_Connection.retrbinary("RETR %s" % Twf_file_to_download, f.write)
									time.sleep(1)
                            except:
								os.remove(os.path.join(theExtractWorkspace,Twf_file_to_download))							
								Twf_file_to_download = Twf_file_to_download.replace("04A", "04B")
								downloadedFile_TFW = os.path.join(theExtractWorkspace,Twf_file_to_download)	
								try:														
									with open(downloadedFile_TFW, "wb") as f:								
										ftp_Connection.retrbinary("RETR %s" % Twf_file_to_download, f.write)
										time.sleep(1)	
								except:
									os.remove(os.path.join(theExtractWorkspace,Twf_file_to_download))							
									Twf_file_to_download = Twf_file_to_download.replace("04B", "05B")
									downloadedFile_TFW = os.path.join(theExtractWorkspace,Twf_file_to_download)	
									try:														
										with open(downloadedFile_TFW, "wb") as f:								
											ftp_Connection.retrbinary("RETR %s" % Twf_file_to_download, f.write)
											time.sleep(1)	
									except:
										addToLog("",True)
                        # Two files were downloaed (or 'extracted') but we really only need a reference to 1 file (thats what the transform expects).. and Arc actually understands the association between the TIF and TWF files automatically
                        extractedFileList = []
                        extractedFileList.append(downloadedFile_TIF)
                        current_Extracted_Obj = {
                                'DateString' : curr_FilePath_Object['DateString'],
                                'DateString_WithMinutes' : curr_FilePath_Object['DateString_WithMinutes'],      # KS Refactor For 30 Min Datasets // Added more detailed DateString
                                'Downloaded_FilePath' : downloadedFile_TIF,
                                'ExtractedFilesList' : convert_Obj_To_List(extractedFileList),
                                'downloadURL' : curr_FilePath_Object['FTP_PathTo_TIF'], #currentURL_ToDownload
                                'FTP_DataObj' : curr_FilePath_Object
                            }
                        ExtractList.append(current_Extracted_Obj)
                        lastBaseRaster = curr_FilePath_Object['BaseRasterName']
                        lastFTPFolder = curr_FilePath_Object['FTPSubFolderPath']
                        counter_FilesDownloaded += 1
                        if counter_FilesDownloaded % 100 == 0:   # if counter_FilesDownloaded % 20 == 0:
                            addToLog("Extract_FTP: Downloaded " + str(counter_FilesDownloaded) + " Rasters ....")

                    except:
                        # If the raster file is missing or an error occurs during transfer..
                        addToLog("Extract_FTP: ERROR.  Error downloading current raster " +  str(curr_FilePath_Object['BaseRasterName']))
                        addToLog(Twf_file_to_download)
                        # KS Refactor for Early Data // Storying the Error Rasters in the return object
                        errorRasters_List.append(str(curr_FilePath_Object['BaseRasterName']))


        except:
            e = sys.exc_info()[0]
            errMsg = "Extract_FTP: ERROR: Could not connect to FTP Server, Error Message: " + str(e)


    addToLog("Extract_FTP: Total number of rasters downloaded: " + str(counter_FilesDownloaded))

    ret_ExtractObj = {
        'StartDateTime':startDateTime,
        'EndDateTime': endDateTime,
        'ExtractList':ExtractList,
        'lastBaseRaster' : lastBaseRaster,
        'lastFTPFolder' : lastFTPFolder,
        'errorRasters_List' : errorRasters_List         # KS Refactor for Early Data // Storying the Error Rasters in the return object
    }

    return ret_ExtractObj

def Extract_Controller_Method(ETL_TransportObject):

    # Check the setup for errors as we go.
    IsError = False
    ErrorMessage = ""

    # Get inputs for the next function


    # Inputs from ETL_TransportObject['SettingsObj']
    try:
        the_FileExtension = ETL_TransportObject['SettingsObj']['Download_File_Extension'] # TRMM_FileExtension # TRMM_File_Extension
        s3BucketRootPath = ETL_TransportObject['SettingsObj']['s3_BucketRootPath']
        s3AccessKey = ETL_TransportObject['SettingsObj']['s3_AccessKeyID']
        s3SecretKey = ETL_TransportObject['SettingsObj']['s3_SecretAccessKey']
        s3BucketName = ETL_TransportObject['SettingsObj']['s3_BucketName']
        s3PathTo_Files = ETL_TransportObject['SettingsObj']['s3_PathTo_TRMM_Files']
        s3_Is_Use_Local_IAM_Role = get_BoolSetting(ETL_TransportObject['SettingsObj']['s3_UseLocal_IAM_Role'])
        regEx_String = ETL_TransportObject['SettingsObj']['RegEx_DateFilterString']
        dateFormat_String = ETL_TransportObject['SettingsObj']['Python_DateFormat']
        extractWorkspace = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['Extract']
    except:
        e = sys.exc_info()[0]
        errMsg = "Extract_Controller_Method: ERROR: Could not get extract inputs, Error Message: " + str(e)
        addToLog(errMsg)
        IsError = True
        ErrorMessage += "|  " + errMsg


    # Get the Start and End Dates
    try:
        varList = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Variable_Dictionary_List']
        GeoDB_Workspace = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['GeoDB_Dataset_Workspace']
        mosaicName = varList[0]['mosaic_name'] # ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Variable_Dictionary_List']
        primaryDateField = varList[0]['primary_date_field']
        mosaicDS = os.path.join(GeoDB_Workspace,mosaicName)
		
        startDateTime = Extract_Support_GetStartDate(primaryDateField,mosaicDS)
		
        # KS Refactor For 30 Min Datasets  (original string for the 3 hour dataset "%Y%m%d%H")
        dateFormat_String = "%Y%m%d%H%M"
        try:
			endDateTime = datetime.datetime.utcnow()
			print endDateTime
			#x=time.strptime("2017-04-21 01:00:00","%Y-%m-%d %H:%M:00")
			#xd=datetime.fromtimestamp(mktime(x))
			#endDateTime = xd			
        except:
			et=datetime.utcnow().strftime("%Y-%m-%d %H:%M:00")
			endDateTime = datetime.strptime(et, "%Y-%m-%d %H:%M:%S")
        #startDateTime_str = datetime.datetime.strptime(startDateTime.strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S")
        startDateTime_str = datetime.datetime.strptime(startDateTime,"%Y-%m-%d %H:%M:%S")		
        endDateTime_str = endDateTime.strftime("%Y-%m-%d %H:%M:%S")
        #startDateTime_str = datetime.datetime.strptime("2018-03-02 11:00:00","%Y-%m-%d %H:%M:%S")
        #startDateTime_str = startDateTime_str.strftime("%Y-%m-%d %H:%M:%S")
        #endDateTime_str = datetime.datetime.strptime("2018-03-03 13:00:00","%Y-%m-%d %H:%M:%S")
        #endDateTime_str = endDateTime_str.strftime("%Y-%m-%d %H:%M:%S")			
    except:
        e = sys.exc_info()[0]
        errMsg = "Extract_Controller_Method: ERROR: Could not get Dates, Error Message: " + str(e)
        addToLog(errMsg)
        IsError = True
        ErrorMessage += "|  " + errMsg
    addToLog("Extract_Controller_Method: Using startDateTime_str : startDateTime : " + str(startDateTime_str) + " : " + str(startDateTime))
    addToLog("Extract_Controller_Method: Using endDateTime_str : endDateTime :  " + str(endDateTime_str) + " : " + str(endDateTime))

    # Execute the Extract Process.
    ExtractResult = Extract_FTP(dateFormat_String, startDateTime_str, endDateTime_str, extractWorkspace)



    if len(ExtractResult['ExtractList']) == 0:
        IsError = True
        ErrorMessage += "|  Extract List contains 0 elements.  No files were extracted."

    # Package up items from the PreETL Step
    returnObj = {
        'ExtractResult': ExtractResult,
        'OldestDateTime': startDateTime,
        'IsError': IsError,
        'ErrorMessage':ErrorMessage
    }

    # Return the packaged items.
    return returnObj


#--------------------------------------------------------------------------
# Transform
#--------------------------------------------------------------------------


# Copy rasters from their scratch location to their final location.
# Called for each extracted item
# KS Refactor For 30 Min Datasets // Added param 'dateSTR_WithMinutes'
def Transform_CopyRaster(coor_system, extractResultObj, varList, dateSTR, dateSTR_WithMinutes, extFileList, rasterOutputLocation, colorMapLocation):

    # KS Refactor for boundary mask
    global g_BoundaryFolder, g_BoundaryFileName
    boundary_FullPath = os.path.join(g_BoundaryFolder, g_BoundaryFileName)

    # Inputs
    # transOptions, varList,extFileList,dateSTR, rasterOutputLocation, the_S3_Info
    # coor_system = transOptions["coordinate_system"] # Defined as, "coordinate_system":"WGS 1984"

    # Gather Inputs

    # Blank output list
    outputVarFileList = []

    # Execute Transform Raster Copy
    try:
        # The way this is set up is if a single zip contains multiple files.. for TRMM, there is only a single file in the zip..
        # Keeping the code as it is, so it can be flexible to handle other cases in the future.
        for varDict in varList:

            #addToLog("Transform_CopyRaster: ----- DEBUG CURRENT ITEM START -----")

            varName = varDict["variable_name"]
            filePrefix = varDict["file_prefix"]
            fileSuffix = varDict["file_suffix"]
            mosaicName = varDict["mosaic_name"]
            primaryDateField = varDict["primary_date_field"]

            # KS Refactor For 30 Min Datasets // Adjusted this to work with the 30 min dataset
            # IMERG Refactor - 5/2015 - Different filename format (partly dependent on the datestring)
            minutePart = dateSTR_WithMinutes[10:12] # Only want the mm (minutes)
            hourPart = dateSTR[8:10] # Only want the hh (not yyyymmddhh)
            imerg_DateSTR = dateSTR[:-2] # Only want the yyyymmdd (not yyyymmddhh)
            current_imerg_S_String = get_IMERG_S_String_From_Hour_And_Minute(int(hourPart), int(minutePart))
            # addToLog("Transform_CopyRaster: Value of datestring for current raster: (dateSTR_WithMinutes): " + str(dateSTR_WithMinutes))

            # Build the name of the raster file we're looking for based on
            #   the configuration for the variable and find it in the list
            #   of files that were extracted
            raster_base_name = filePrefix + imerg_DateSTR + current_imerg_S_String + fileSuffix
            raster_base_name=raster_base_name.replace("03E","04A")
            # Find the file in the list of downloaded files associated with
            #   the current variable
            raster_file = ""
            raster_name = ""
            raster_path = ""
            for aName in extFileList:
                currBaseName = os.path.basename(aName)
                if currBaseName == raster_base_name:
                    raster_file = aName
                    raster_name = raster_base_name
                    raster_path = os.path.dirname(raster_file)
                else:
					aName=aName.replace("04A","04B")
					raster_base_name=raster_base_name.replace("04A","04B")
					currBaseName = os.path.basename(aName)
					if currBaseName == raster_base_name:
						raster_file = aName
						raster_name = raster_base_name
						raster_path = os.path.dirname(raster_file)			
					else:
						aName=aName.replace("04B","05B")
						raster_base_name=raster_base_name.replace("04B","05B")
						currBaseName = os.path.basename(aName)
						if currBaseName == raster_base_name:
							raster_file = aName
							raster_name = raster_base_name
							raster_path = os.path.dirname(raster_file)							
					
            # If we don't find the file in the list of downloaded files,
            #   skip this variable and move on; otherwise, process the file
            if not arcpy.Exists(raster_file):
                addToLog("Transform_CopyRaster No file found for expected raster_base_name, " + str(raster_base_name) + "...skipping...")
            else:			
                # Add the output raster location for the full raster path
                out_raster = os.path.join(rasterOutputLocation, raster_name)
                # Perform the actual conversion (If the file already exists, this process breaks.)
                if not arcpy.Exists(out_raster):
					try:	
						if arcpy.Exists(raster_file):						
					
							arcpy.CopyRaster_management(raster_file, out_raster)    # This operation DOES overwrite an existing file (so forecast items get overwritten by actual items when this process happens)
					except:
						#print str(sys.exc_info()[0])				
						raster_file=raster_file.replace("04A","04B")
						out_raster=out_raster.replace("04A","04B")			
						try:
							if arcpy.Exists(raster_file):						
								arcpy.CopyRaster_management(raster_file, out_raster)	
						except:
							#print str(sys.exc_info()[0])				
							raster_file=raster_file.replace("04B","05B")
							out_raster=out_raster.replace("04B","05B")			
							try:
								if arcpy.Exists(raster_file):							
									arcpy.CopyRaster_management(raster_file, out_raster)	
							except:
								print sys.exc_info()[0]
						
						
					addToLog("Transform_CopyRaster: Copied "+ os.path.basename(raster_file)+" to "+str(out_raster), True)
                else:
					
                    addToLog("Transform_CopyRaster: Raster, "+ os.path.basename(raster_file)+" already exists at output location of: "+str(out_raster), True)

                # Apply a color map
                try:
                    #out_raster=out_raster.replace("\\\\","//")
                    if arcpy.Exists(out_raster):
						arcpy.AddColormap_management(out_raster, "#", colorMapLocation)
						addToLog("Transform_CopyRaster: Color Map has been applied to "+str(out_raster), True)
                except:
                    addToLog("Transform_CopyRaster: Error Applying color map to raster : " + str(out_raster) + " ArcPy Error Message: " + str(arcpy.GetMessages()))
                # Define the coordinate system
                sr = arcpy.SpatialReference(coor_system)
                arcpy.DefineProjection_management(out_raster, sr)
                addToLog("Transform_CopyRaster: Defined coordinate system: "+ str(sr.name), True)
                # Append the output file and it's associated variable to the
                #   list of files processed
                currRastObj = {
                    "out_raster_file_location":out_raster,
                    "mosaic_ds_name":mosaicName,
                    "primary_date_field":primaryDateField
                }
                outputVarFileList.append(currRastObj)

            # END OF Loop (for varDict in varList:)
    except:
        e = sys.exc_info()[0]
        addToLog("Transform_CopyRaster: ERROR: Something went wrong during the transform process, Error Message: " + str(e),True)

    # Return the output list
    return outputVarFileList

def Transform_Controller_Method(ETL_TransportObject):
    # Do a "Transform" Process

    # Gather inputs
    coor_system = ETL_TransportObject['SettingsObj']['TRMM_RasterTransform_CoordSystem']
    extractResultObj = ETL_TransportObject['Extract_Object']['ResultsObject']
    varList = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Variable_Dictionary_List']
    rasterOutputLocation = ETL_TransportObject['SettingsObj']['Raster_Final_Output_Location']
    colorMapLocation = ETL_TransportObject['SettingsObj']['trmm3Hour_ColorMapLocation']

    # For each item in the extract list.. call this function
    TransformResult_List = []
    current_ExtractList = ETL_TransportObject['Extract_Object']['ResultsObject']['ExtractResult']['ExtractList']
    for currentExtractItem in current_ExtractList:
        current_dateSTR = currentExtractItem['DateString']
        current_dateSTR_WithMinutes = currentExtractItem['DateString_WithMinutes']  # KS Refactor For 30 Min Datasets // Added date string with minutes
        current_extFileList = currentExtractItem['ExtractedFilesList']
        # KS Refactor For 30 Min Datasets // Added date string with minutes as 5th parameter
        Transformed_File_List = Transform_CopyRaster(coor_system, extractResultObj, varList, current_dateSTR, current_dateSTR_WithMinutes, current_extFileList, rasterOutputLocation, colorMapLocation)
        if len(Transformed_File_List) == 0:
            pass
        else:
            CurrentTransObj = {
                'Transformed_File_List':Transformed_File_List,
                'date_string':current_dateSTR,
                'date_string_WithMinutes':current_dateSTR_WithMinutes   # KS Refactor For 30 Min Datasets // Added date string with minutes
            }
            TransformResult_List.append(CurrentTransObj)

    # Check the above setup for errors
    IsError = False
    ErrorMessage = ""

    if len(TransformResult_List) == 0:
        IsError = True
        ErrorMessage += "|  Transform List contains 0 elements.  No files were transformed."
    # Package up items from the PreETL Step
    returnObj = {
        'TransformResult_List': TransformResult_List,
        'IsError': IsError,
        'ErrorMessage':ErrorMessage
    }

    # Return the packaged items.
    return returnObj


#--------------------------------------------------------------------------
# Load
#--------------------------------------------------------------------------
def Load_Dataset(transFileList, geoDB_MosaicDataset_Workspace, regExp_Pattern, date_Format, coor_system):


    mdWS = geoDB_MosaicDataset_Workspace
    # Load each raster into its appropriate mosaic dataset
    numLoaded = 0

    # KS Refactor for Early Data // Keeping track of the last datetime object that was loaded
    latest_Loaded_DateTimeObject = datetime.datetime.strptime("2010", "%Y") # Some date that was way before any of this data ever existed..
    for fileDict in transFileList:

        rasterFile = fileDict["out_raster_file_location"]       # Filesystem folder that holds raster files.
        rasterName = os.path.basename(rasterFile).replace(".tif","")     # different filename schema uses this -->  # os.path.basename(rasterFile).split(".")[0]
        addToLog("Load_Dataset: rasterName " + str(rasterName), True)
        mosaicDSName = fileDict["mosaic_ds_name"]
        primaryDateField = fileDict["primary_date_field"]
        mosaicDS = os.path.join(mdWS, mosaicDSName)             # GeoDB/DatasetName
        addError = False

        # For now, skip the file if the mosaic dataset doesn't exist.  Could
        #   be updated to create the mosaic dataset if it's missing
        if not arcpy.Exists(mosaicDS):
            addToLog("Load_Dataset: Mosaic dataset "+str(mosaicDSName)+", located at, " +str(mosaicDS)+" does not exist.  Skipping "+os.path.basename(rasterFile))
        else:
            try:
                # Add raster to mosaic dataset
                
                addError = False
                sr = arcpy.SpatialReference(coor_system)
                #addToLog("file size: "+ str(os.stat(rasterFile).st_size)+"Mosaic ds: "+mosaicDS+"----Raster name: "+rasterFile)				
                # if os.stat(rasterFile).st_size > 0:
					# if numloaded>0 and numLoaded%10==0:
						# addToLog("Loading "+rasterFile+' numloaded '+str(numLoaded))					
                arcpy.AddRastersToMosaicDataset_management(mosaicDS, "Raster Dataset", rasterFile,\
														   "NO_CELL_SIZES", "NO_BOUNDARY", "NO_OVERVIEWS",\
														   "2", "#", "#", "#", "#", "NO_SUBFOLDERS",\
														   "OVERWRITE_DUPLICATES", "NO_PYRAMIDS", "NO_STATISTICS",\
														   "NO_THUMBNAILS", "Add Raster Datasets","#")
                addToLog("Load_Dataset: Loaded " +str(rasterFile)+" to mosaic dataset "+str(mosaicDSName))
                numLoaded += 1


            except:
                #print(arcpy.GetMessages(2))
                e = sys.exc_info()[0]
                addToLog("Load_Dataset: ERROR: Something went wrong when adding the raster to the mosaic dataset. Error Message: " + str(e) + " ArcPy Messages" + str(arcpy.GetMessages(2)))
                addError = True

            if not addError:


                # Load the Attributes into the Attribute table

                # Build attribute and value lists
                attrNameList = []
                attrExprList = []

                # Build a list of attribute names and expressions to use with
                #   the ArcPy Data Access Module cursor below
                HC_AttrName = "timestamp"
				
                # KS Refactor For 30 Min Datasets // So we need to know the current datetime
                # Override the Dateformat (for 30 min datasets)
                date_Format = "%Y%m%d%H%M"
                # Get the datetime (including minutes) from the rastername
                rasterName_ToParse = rasterName                 # '3B-HHR-L.MS.MRG.3IMERG.20150421-S160000-E162959.0960.V03E.30min'
                rastParts = rasterName_ToParse.split('.')       # above string split into parts
                datetimeCode = rastParts[4]                     # '20150421-S160000-E162959'
                minuteCode = rastParts[5]                       # '0960'
                dateSection = datetimeCode.split('-')[0]        # '20150421'
                int_TheHours = int(minuteCode)/60               # 16
                int_TheMinutes = int(minuteCode)%60             # 0
                # Put it all together
                CurrentDateTime_STR = dateSection
                if int_TheHours < 10:
                    CurrentDateTime_STR += "0"
                CurrentDateTime_STR += str(int_TheHours)
                if int_TheMinutes < 10:
                    CurrentDateTime_STR += "0"
                CurrentDateTime_STR += str(int_TheMinutes)      # '201504211600'        (And then a 30 min example  '201504211630')
                CurrentDateTime = datetime.datetime.strptime(CurrentDateTime_STR, date_Format)
                #CurrentDateTime = CurrentDateTime.strftime(date_Format)
				
                attrNameList.append(HC_AttrName)
                attrExprList.append(CurrentDateTime)
                # Quick fix, Adding the extra fields
                # 'start_datetime' and 'end_datetime'
                # KS Refactor For 30 Min Datasets // Need to only delta 15 minutes (and not 1.5 hours)
                CurrentDateTime_Minus_1hr30min = CurrentDateTime - datetime.timedelta(minutes=15)		
                CurrentDateTime_Minus_1hr30min = datetime.datetime.strptime(CurrentDateTime_Minus_1hr30min.strftime(date_Format), date_Format)
                attrNameList.append('start_datetime')
                attrExprList.append(CurrentDateTime_Minus_1hr30min)
				
                # KS Refactor For 30 Min Datasets // Need to only delta 15 minutes (and not 1.5 hours)
                #CurrentDateTime_Plus_1hr30min = CurrentDateTime + datetime.timedelta(hours=1.5)
                CurrentDateTime_Plus_1hr30min = CurrentDateTime + datetime.timedelta(minutes=15)
                CurrentDateTime_Plus_1hr30min = datetime.datetime.strptime(CurrentDateTime_Plus_1hr30min.strftime(date_Format), date_Format)
                attrNameList.append('end_datetime')				
                attrExprList.append(CurrentDateTime_Plus_1hr30min)
                # EARLY / LATE COLUMN
                attrNameList.append('Data_Age')
                attrExprList.append('LATE')

                # Update the attributes with their configured expressions
                #   (ArcPy Data Access Module UpdateCursor)
                try:
                    wClause = arcpy.AddFieldDelimiters(mosaicDS,"name")+" = '"+rasterName+"'"
					
                    with arcpy.da.UpdateCursor(mosaicDS, attrNameList, wClause) as cursor:
					
                        for row in cursor:
                            for idx in range(len(attrNameList)):
                                row[idx] = attrExprList[idx]
                            cursor.updateRow(row)

                    addToLog("Load_Dataset: Calculated attributes for raster", True)
                    del cursor

                    # KS Refactor for Early Data // Keeping track of the last datetime object that was loaded
                    # At this point in the code, the new Raster was JUST loaded.. so lets do the little compare operation on the dates and see if this date is the one we want to keey (we want the most recent date)
                    if CurrentDateTime > latest_Loaded_DateTimeObject:
                        latest_Loaded_DateTimeObject = CurrentDateTime

                # Handle errors for calculating attributes
                except:
                    e = sys.exc_info()[0]
                    addToLog("Load_Dataset: ERROR: Error calculating attributes for raster"+str(rasterFile)+"  Error Message: "+arcpy.GetMessages() + str(e))
    
	                # Calculate statistics on the mosaic dataset

    retObj = {
        'NumberLoaded': numLoaded,
        'latest_Loaded_DateTimeObject' : latest_Loaded_DateTimeObject       # KS Refactor for Early Data // Keeping track of the last datetime object that was loaded
    }

    return retObj


def Load_Do_ETL_For_EarlyDataset(LoadResult_List, error_Late_Rasters_List, extractWorkspace, ETL_TransportObject):
    try:
		# First, remove old / outdated load items. (remove anything with an 'E' for 'Early' in the filename for any rasters that we have 'Late' data for)
		addToLog("Load_Do_ETL_For_EarlyDataset: ==== CLEAN OutDated Early rasters from the geodb and filesystem =====")
		latestDate_Loaded_ForLateItems = datetime.datetime.strptime("2009", "%Y") # Some date that was way before any of this data ever existed..
		for currentLoadResult in LoadResult_List:
			currentDateToCheck = currentLoadResult['latest_Loaded_DateTimeObject']
			if currentDateToCheck > latestDate_Loaded_ForLateItems:
				latestDate_Loaded_ForLateItems = currentDateToCheck
		addToLog("Load_Do_ETL_For_EarlyDataset: Cleanup: Date to use to remove outdated early datasets is: " + str(latestDate_Loaded_ForLateItems))
		# Now write the query..
		# Gathering what should be settings
		config_FileSystem_Folder_LocationOfRasters = r""
		config_PathToGeoDB = r"E:\SERVIR\Data\Global"
		config_GeoDB_Name = "IMERG_SR3857.gdb"
		config_FeatureClass_Name = "IMERG"

		# Gathering variables
		path_To_FileSystemRasters = config_FileSystem_Folder_LocationOfRasters
		path_To_FeatureClass = os.path.join(config_PathToGeoDB, config_GeoDB_Name, config_FeatureClass_Name)
		# Clean GeoDB and Gather list of names to remove from filesystem
		list_Of_Raster_Names_ToRemove = []
		primaryDateField = 'timestamp'
		rasterNameField = 'Name'
		theFields = [primaryDateField, rasterNameField]
		# For performance improvments., This stuff here should be replaced with a proper arcpy sql statement and use of 'arcpy.DeleteRows_management'
		theDeleteCounter = 0
		names_To_Delete_List = []
		# This part removes items from the GeoDB
		
		try:
			with arcpy.da.UpdateCursor(path_To_FeatureClass, theFields) as cursor:
				for row in cursor:		
					currentRasterDate = row[0]
					currentRasterName = row[1]
					# We want to only remove items that have the 'E' for 'Early' and that are same age or older than the last date added from the 'Late' dataset
					if currentRasterName[7] == 'E':
						if currentRasterDate <= latestDate_Loaded_ForLateItems:
							# DELETE ROW HERE!! INCREMENT COUNTER
							try:
								cursor.deleteRow()
								theDeleteCounter += 1
								names_To_Delete_List.append(currentRasterName)
								addToLog("Load_Do_ETL_For_EarlyDataset: Deleted outdated Early Item: " + str(currentRasterName) + " from the GeoDB")
							except:
								e = sys.exc_info()[0]
								addToLog("Load_Do_ETL_For_EarlyDataset: ERROR Deleting outdated Early item: " + str(currentRasterName) + " from the GeoDB, System Error Message: " + str(e))
		except:
			e = sys.exc_info()[0]
			addToLog("Update Cursor exception"""+str(e))	
		del cursor # Remove the cursor... so the locks are gone from the GeoDB?

		addToLog("Load_Do_ETL_For_EarlyDataset: Cleanup: Removed a total of " + str(theDeleteCounter) + " outdated early rasters from the GeoDB")
		addToLog("Load_Do_ETL_For_EarlyDataset: About to execute micro ETL process for Early Data")
		
		pkl_file = open('config.pkl', 'r')
		myConfig = pickle.load(pkl_file) #store the data from config.pkl file
		pkl_file.close()
		
		# Gathering needed vars for Extract Process
		list_Of_Early_Rasters_ToGet = error_Late_Rasters_List
		
		the_FTP_Host = myConfig['ftp_host']
		the_FTP_SubFolderPath = myConfig['ftp_subfolder']
		the_FTP_UserName = myConfig['ftp_user']
		the_FTP_UserPass = myConfig['ftp_pswrd']
		root_FTP_Path = "ftp://" + str(the_FTP_Host) + "/" + the_FTP_SubFolderPath

		# Extract, Transform, Load
		if len(list_Of_Early_Rasters_ToGet) > 0:
			addToLog("Load_Do_ETL_For_EarlyDataset: DEBUG: list_Of_Early_Rasters_ToGet: " + str(list_Of_Early_Rasters_ToGet))


			# Extract
			addToLog("Load_Do_ETL_For_EarlyDataset: ==== Early_Extract Start")
			extract_List_EarlyItems = []
			earlyItems_DownloadCounter = 0 # This counts Datasets, not individual files (so in this case, 2 files per dataset (tif and twf))
			try:

				addToLog("Load_Do_ETL_For_EarlyDataset: Extract:  Connecting to FTP to get all Early Rasters")
				ftp_Connection = ftplib.FTP(the_FTP_Host,the_FTP_UserName,the_FTP_UserPass)
				time.sleep(1)
				addToLog("Load_Do_ETL_For_EarlyDataset: Extract:  FTP, Changing folder to : " + str(the_FTP_SubFolderPath), True)
				ftp_Connection.cwd("/" + the_FTP_SubFolderPath)
				time.sleep(1)

				for current_Error_Raster in list_Of_Early_Rasters_ToGet:
					# Addjust the name so it has "E" in it instead of "L"
					# 'current_Error_Raster' looks like this "3B-HHR-L.MS.MRG.3IMERG.20150802-S083000-E085959.0510.V03E.30min"
					base_Name_of_Early_Raster = ""
					base_Name_of_Early_Raster += current_Error_Raster[:7]   # '3B-HHR-'
					base_Name_of_Early_Raster += "E"
					base_Name_of_Early_Raster += current_Error_Raster[8:]   # '.MS.MRG.3IMERG.20150802-S083000-E085959.0510.V03E.30min'
					# Result basename example: '3B-HHR-E.MS.MRG.3IMERG.20150802-S083000-E085959.0510.V03E.30min'

					# Get the Datetime object from the filename.. # datetime.datetime(2015, 8, 2, 8, 30)
					current_EarlyRaster_DateObj = Update_Accumulations_Get_DateTime_From_BaseFileName(base_Name_of_Early_Raster)

					Tif_file_to_download = base_Name_of_Early_Raster + ".tif"
					Twf_file_to_download = base_Name_of_Early_Raster + ".tfw"
					downloadedFile_TIF = os.path.join(extractWorkspace,Tif_file_to_download)
					downloadedFile_TFW = os.path.join(extractWorkspace,Twf_file_to_download)

					try:
						# Attempt to download the TIF and World File (Tfw)
						try:
							downloadedFile_TIF = os.path.join(extractWorkspace,Tif_file_to_download)
							with open(downloadedFile_TIF, "wb") as f:			
							
								ftp_Connection.retrbinary("RETR %s" % Tif_file_to_download, f.write)
								time.sleep(1)
						except:
							Tif_file_to_download = Tif_file_to_download.replace("03E", "04A")
							downloadedFile_TIF = os.path.join(extractWorkspace,Tif_file_to_download)		
							try:							
								with open(downloadedFile_TIF, "wb") as f:			
									ftp_Connection.retrbinary("RETR %s" % Tif_file_to_download, f.write)
									time.sleep(1)
							except:
								Tif_file_to_download = Tif_file_to_download.replace("04A", "04B")
								downloadedFile_TIF = os.path.join(extractWorkspace,Tif_file_to_download)
								try:							
									with open(downloadedFile_TIF, "wb") as f:			
										ftp_Connection.retrbinary("RETR %s" % Tif_file_to_download, f.write)
										time.sleep(1)	
								except:
									Tif_file_to_download = Tif_file_to_download.replace("04B", "05B")
									downloadedFile_TIF = os.path.join(extractWorkspace,Tif_file_to_download)
									try:							
										with open(downloadedFile_TIF, "wb") as f:			
											ftp_Connection.retrbinary("RETR %s" % Tif_file_to_download, f.write)
											time.sleep(1)	
									except:
										addToLog("",True)												
						
						try:
							downloadedFile_TFW = os.path.join(extractWorkspace,Twf_file_to_download)
							with open(downloadedFile_TFW, "wb") as f:								
								ftp_Connection.retrbinary("RETR %s" % Twf_file_to_download, f.write)
								time.sleep(1)
						except:					
							Twf_file_to_download = Twf_file_to_download.replace("03E", "04A")
							downloadedFile_TFW = os.path.join(extractWorkspace,Twf_file_to_download)
							try:														
								with open(downloadedFile_TFW, "wb") as f:								
									ftp_Connection.retrbinary("RETR %s" % Twf_file_to_download, f.write)
									time.sleep(1)
							except:
								Twf_file_to_download = Twf_file_to_download.replace("04A", "04B")
								downloadedFile_TFW = os.path.join(extractWorkspace,Twf_file_to_download)	
								try:														
									with open(downloadedFile_TFW, "wb") as f:								
										ftp_Connection.retrbinary("RETR %s" % Twf_file_to_download, f.write)
										time.sleep(1)	
								except:
									Twf_file_to_download = Twf_file_to_download.replace("04B", "05B")
									downloadedFile_TFW = os.path.join(extractWorkspace,Twf_file_to_download)	
									try:														
										with open(downloadedFile_TFW, "wb") as f:								
											ftp_Connection.retrbinary("RETR %s" % Twf_file_to_download, f.write)
											time.sleep(1)	
									except:
										addToLog("",True)


						extractObj = {
							'downloadedFile_TIF' : downloadedFile_TIF,
							'downloadedFile_TFW' : downloadedFile_TFW,
							'base_Name_of_Early_Raster' : base_Name_of_Early_Raster,
							'current_EarlyRaster_DateObj' : current_EarlyRaster_DateObj
						}
						extract_List_EarlyItems.append(extractObj)
						earlyItems_DownloadCounter += 1

					except:
						e = sys.exc_info()[0]
						addToLog("Load_Do_ETL_For_EarlyDataset: Extract: ERROR Downloading Early Raster: " + str(base_Name_of_Early_Raster) + " System Error Message: " + str(e))



			except:
				e = sys.exc_info()[0]
				addToLog("Load_Do_ETL_For_EarlyDataset: Extract:  ERROR Downloading Early Rasters, System Error Message: " + str(e))

			# Transform
			addToLog("Load_Do_ETL_For_EarlyDataset: ==== Early_Transform Start")

			# Blank output list
			output_Early_Transform_FileList = []    # outputVarFileList

			# Other Needed Vars
			rasterOutputLocation = ETL_TransportObject['SettingsObj']['Raster_Final_Output_Location']
			coor_system = ETL_TransportObject['SettingsObj']['TRMM_RasterTransform_CoordSystem']
			colorMapLocation = ETL_TransportObject['SettingsObj']['trmm3Hour_ColorMapLocation']
			varList = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Variable_Dictionary_List']
			for current_Early_Transform_Item in extract_List_EarlyItems:
				base_Name_of_Early_Raster = current_Early_Transform_Item['base_Name_of_Early_Raster']
				current_EarlyRaster_DateObj = current_Early_Transform_Item['current_EarlyRaster_DateObj']
				downloadedFile_TIF = current_Early_Transform_Item['downloadedFile_TIF']

				raster_name = base_Name_of_Early_Raster + ".tif"
				out_raster = os.path.join(rasterOutputLocation, raster_name)
				raster_file = downloadedFile_TIF # NEED TO GET THE FULL PATH FILE NAME.. it might be, 'downloadedFile_TIF'

				if not arcpy.Exists(out_raster):
					arcpy.CopyRaster_management(raster_file, out_raster)
					addToLog("Load_Do_ETL_For_EarlyDataset: Transform: Copied " + str(raster_file) + " to " + str(out_raster))
				else:
					addToLog("Load_Do_ETL_For_EarlyDataset: Transform: Raster, " + str(raster_file)+" already exists at output location of: "+str(out_raster))

				# Apply a color map
				try:
					arcpy.AddColormap_management(out_raster, "#", colorMapLocation)
					addToLog("Load_Do_ETL_For_EarlyDataset: Transform: Color Map has been applied to "+str(out_raster))
				except:
					e = sys.exc_info()[0]
					addToLog("Load_Do_ETL_For_EarlyDataset: Transform: Error Applying color map to raster : " + str(out_raster) + " ArcPy Error Message: " + str(arcpy.GetMessages()) + ", System Error Message: " + str(e))

				# Define the coordinate system
				sr = arcpy.SpatialReference(coor_system)
				arcpy.DefineProjection_management(out_raster, sr)
				addToLog("Load_Do_ETL_For_EarlyDataset: Transform: Defined coordinate system: "+ str(sr.name))

				currRastObj = {
					"out_raster_file_location":out_raster,
					"current_EarlyRaster_DateObj" : current_EarlyRaster_DateObj
				}
				output_Early_Transform_FileList.append(currRastObj)
				#ETL_TransportObject


			# Load
			addToLog("Load_Do_ETL_For_EarlyDataset: ==== Early_Load Start")
			# Gathering variables
			mosaicDSName = config_FeatureClass_Name  # should be: "IMERG"   #fileDict["mosaic_ds_name"]
			# Variable Already defined!:    # primaryDateField = fileDict["primary_date_field"]
			mosaicDS = path_To_FeatureClass # mosaicDS = os.path.join(mdWS, mosaicDSName)             # GeoDB/DatasetName

			# Load Counter
			numLoaded = 0

			for fileDict in output_Early_Transform_FileList: # for fileDict in transFileList:
				current_EarlyRaster_DateObj = fileDict["current_EarlyRaster_DateObj"]
				rasterFile = fileDict["out_raster_file_location"]
				rasterName = os.path.basename(rasterFile).replace(".tif","")
				addToLog("Load_Do_ETL_For_EarlyDataset: Load: about to load raster (rasterName): " + str(rasterName))

				addError = False

				# For now, skip the file if the mosaic dataset doesn't exist.  Could
				#   be updated to create the mosaic dataset if it's missing
				if not arcpy.Exists(mosaicDS):
					addToLog("Load_Do_ETL_For_EarlyDataset: Load: Mosaic dataset "+str(mosaicDSName)+", located at, " +str(mosaicDS)+" does not exist.  Skipping "+os.path.basename(rasterFile))
				else:
					try:
						# Add raster to mosaic dataset
						addError = False
						sr = arcpy.SpatialReference(coor_system)
						if os.stat(rasterFile).st_size > 0:						
							arcpy.AddRastersToMosaicDataset_management(mosaicDS, "Raster Dataset", rasterFile,\
																		"NO_CELL_SIZES", "NO_BOUNDARY", "NO_OVERVIEWS",\
																		"2", "#", "#", "#", "#", "NO_SUBFOLDERS",\
																		"OVERWRITE_DUPLICATES", "NO_PYRAMIDS", "NO_STATISTICS",\
																		"NO_THUMBNAILS", "Add Raster Datasets","#")
							addToLog("Load_Do_ETL_For_EarlyDataset: Load: Added " +str(rasterFile)+" to mosaic dataset "+str(mosaicDSName))
							numLoaded += 1

					except:
						e = sys.exc_info()[0]
						addToLog("Load_Do_ETL_For_EarlyDataset: Load: ERROR: Something went wrong when adding the raster to the mosaic dataset. Error Message: " + str(e) + " ArcPy Messages: " + str(arcpy.GetMessages(2)))
						addError = True

					if not addError:
						# Calculate statistics on the mosaic dataset
						try:
							arcpy.CalculateStatistics_management(mosaicDS,1,1,"#","SKIP_EXISTING","#")
							addToLog("Load_Do_ETL_For_EarlyDataset: Load: Calculated statistics on mosaic dataset "+str(mosaicDSName), True)
							pass

						# Handle errors for calc statistics
						except:
							e = sys.exc_info()[0]
							addToLog("Load_Do_ETL_For_EarlyDataset: Load: ERROR: Error calculating statistics on mosaic dataset "+str(mosaicDSName)+"  Error Message: " + str(e) + " ArcPy Messages: " + str(arcpy.GetMessages(2)))
							pass

						# Build attribute and value lists
						attrNameList = []
						attrExprList = []

						# Build a list of attribute names and expressions to use with
						#   the ArcPy Data Access Module cursor below
						HC_AttrName = "timestamp"
						CurrentDateTime = current_EarlyRaster_DateObj

						attrNameList.append(HC_AttrName)
						attrExprList.append(CurrentDateTime)

						CurrentDateTime_Minus_1hr30min = CurrentDateTime - datetime.timedelta(minutes=15)
						attrNameList.append('start_datetime')
						attrExprList.append(CurrentDateTime_Minus_1hr30min)
						CurrentDateTime_Plus_1hr30min = CurrentDateTime + datetime.timedelta(minutes=15)
						attrNameList.append('end_datetime')
						attrExprList.append(CurrentDateTime_Plus_1hr30min)

						# EARLY / LATE COLUMN
						attrNameList.append('Data_Age')
						attrExprList.append('EARLY')


						try:
							wClause = arcpy.AddFieldDelimiters(mosaicDS,"name")+" = '"+rasterName+"'"
							with arcpy.da.UpdateCursor(mosaicDS, attrNameList, wClause) as cursor:
								for row in cursor:
									for idx in range(len(attrNameList)):
										row[idx] = attrExprList[idx]
										addToLog("Load_Do_ETL_For_EarlyDataset: Load: DEBUG: Updated Attribute: " + str(attrNameList[idx]) + ", with value: " + str(attrExprList[idx]))


									cursor.updateRow(row)

							addToLog("Load_Do_ETL_For_EarlyDataset: Load: Calculated attributes for raster")
							del cursor



						# Handle errors for calculating attributes
						except:
							e = sys.exc_info()[0]
							addToLog("Load_Do_ETL_For_EarlyDataset: Load: ERROR: Error calculating attributes for raster"+str(rasterFile)+"  Error Message: " + str(e))


		else:
			addToLog("Load_Do_ETL_For_EarlyDataset: No items in list_Of_Early_Rasters_ToGet.  Micro ETL process skipped!")
    except:
		e = sys.exc_info()[0]
		addToLog(str(e))
    return "Load_Do_ETL_For_EarlyDataset Results"

def Load_Controller_Method(ETL_TransportObject):
    # Gather inputs	
    GeoDB_Workspace = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['GeoDB_Dataset_Workspace'] #['SettingsObj']['Raster_Final_Output_Location']
    theRegEx = ETL_TransportObject['SettingsObj']['RegEx_DateFilterString']
    theDateFormat = ETL_TransportObject['SettingsObj']['Python_DateFormat']
    coor_system = ETL_TransportObject['SettingsObj']['TRMM_RasterTransform_CoordSystem']
    # For each item in the Transform list.. call this function
    LoadResult_List = []
    current_TransformList = ETL_TransportObject['Transform_Object']['ResultsObject']['TransformResult_List']
	
    for currentTransformItem in current_TransformList:
        current_TransFileList = currentTransformItem['Transformed_File_List'] # transFileList
        current_LoadResultObj = Load_Dataset(current_TransFileList, GeoDB_Workspace, theRegEx, theDateFormat, coor_system)
        LoadResult_List.append(current_LoadResultObj)
    # KS Refactor for Early Data
	
    error_Late_Rasters_List = ETL_TransportObject['Extract_Object']['ResultsObject']['ExtractResult']['errorRasters_List']
    extractWorkspace = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['Extract']
    try:
        earlyDataset_ETL_Result = Load_Do_ETL_For_EarlyDataset(LoadResult_List, error_Late_Rasters_List, extractWorkspace, ETL_TransportObject)
    except:
        e = sys.exc_info()[0]
        addToLog("Load_Controller_Method: ERROR: Uncaught error while executing function Load_Do_ETL_For_EarlyDataset(params) System Error Message: " + str(e))



    # Check the above setup for errors
    IsError = False
    ErrorMessage = ""

    if len(LoadResult_List) == 0:
        IsError = True
        ErrorMessage += "|  Load List contains 0 elements.  No items were Loaded."


    # Package up items from the PreETL Step
    returnObj = {
        'LoadResult_List': LoadResult_List,
        'IsError': IsError,
        'ErrorMessage':ErrorMessage
    }

    # Return the packaged items.
    return returnObj

#--------------------------------------------------------------------------
# Post ETL
#   Processes that must be performed after the ETL process.
#   This may also include operations on the data which are independent of the
#   ETL process.  For example, CREST's insert line items to seperate postgres
#   DB operations.
#--------------------------------------------------------------------------

# See "Pre ETL" Section for the format of these functions
def PostETL_ExampleSupportMethod():
    pass


#3B42RT.2014062509.7.03hr.tfw	96 B	6/25/14 1:28:00 PM
#3B42RT.2014062509.7.03hr.tif	105 kB	6/25/14 1:28:00 PM
#3B42RT.2014062509.7.1day.tfw	96 B	6/25/14 1:28:00 PM
#3B42RT.2014062509.7.1day.tif	302 kB	6/25/14 1:28:00 PM
#3B42RT.2014062509.7.3day.tfw	96 B	6/25/14 1:28:00 PM
#3B42RT.2014062509.7.3day.tif	519 kB	6/25/14 1:28:00 PM
#3B42RT.2014062509.7.7day.tfw	96 B	6/25/14 1:28:00 PM
#3B42RT.2014062509.7.7day.tif	733 kB	6/25/14 1:28:00 PM
# lastRasterName # Expecting something like : "3B42RT.2014062509.7.03hr"
# whichComposite # Expecting something like : "1day" , "3day", "7day"
# ftpSubfolder # Expecting something like : "/pub/gis/201406"


# Stops the TRMM services, runs the custom raster generation routine, then restarts the TRMM services
def PostETL_Do_Update_Service_And_Custom_Rasters(PostETL_CustomRaster_Params, service_Options_List, ETL_TransportObject):
    # For each service, Stop them all
    addToLog("PostETL_Do_Update_Service_And_CustomRasters: About to stop all TRMM related services")
    for current_Service in service_Options_List:
        current_Description = current_Service['Description']
        current_AdminDirURL = current_Service['admin_dir_URL']
        current_Username = current_Service['username']
        current_Password = current_Service['password']
        current_FolderName = current_Service['folder_name']
        current_ServiceName = current_Service['service_name']
        current_ServiceType = current_Service['service_type']

        # Try and stop each service
        try:
		
            # Get a token from the Administrator Directory
            tokenParams = urllib.urlencode({"f":"json","username":current_Username,"password":current_Password,"client":"requestip"})
			
            tokenResponse = urllib.urlopen(current_AdminDirURL+"/generateToken?",tokenParams).read()
			
            tokenResponseJSON = json.loads(tokenResponse)
			
            token = tokenResponseJSON["token"]

            # Attempt to stop the current service
            stopParams = urllib.urlencode({"token":token,"f":"json"})
            stopResponse = urllib.urlopen(current_AdminDirURL+"/services/"+current_FolderName+"/"+current_ServiceName+"."+current_ServiceType+"/stop?",stopParams).read()
            stopResponseJSON = json.loads(stopResponse)
            stopStatus = stopResponseJSON["status"]

            if stopStatus <> "success":
                addToLog("PostETL_Do_Update_Service_And_CustomRasters: Unable to stop service "+str(current_FolderName)+"/"+str(current_ServiceName)+"/"+str(current_ServiceType)+" STATUS = "+stopStatus)
            else:
                addToLog("PostETL_Do_Update_Service_And_CustomRasters: Service: " + str(current_ServiceName) + " has been stopped.")

        except:
            e = sys.exc_info()[0]
            addToLog("PostETL_Do_Update_Service_And_CustomRasters: ERROR, Stop Service failed for " + str(current_ServiceName) + ", System Error Message: "+ str(e)+ arcpy.GetMessages())



    # Run the code for creating custom rasters
    # addToLog("PostETL_Do_Update_Service_And_CustomRasters: About to update Custom Rasters")
    # try:
        # PostETL_Support_Build_Custom_Rasters(PostETL_CustomRaster_Params, ETL_TransportObject)
    # except:
        # e = sys.exc_info()[0]
        # addToLog("PostETL_Do_Update_Service_And_CustomRasters: ERROR, Something went wrong while building TRMM Custom Rasters, System Error Message: "+ str(e))



    # For each service, Start them all
    addToLog("PostETL_Do_Update_Service_And_CustomRasters: About to restart all TRMM related services")
    for current_Service in service_Options_List:
        current_Description = current_Service['Description']
        current_AdminDirURL = current_Service['admin_dir_URL']
        current_Username = current_Service['username']
        current_Password = current_Service['password']
        current_FolderName = current_Service['folder_name']
        current_ServiceName = current_Service['service_name']
        current_ServiceType = current_Service['service_type']

        # Try and start each service
        try:
            # Get a token from the Administrator Directory
            tokenParams = urllib.urlencode({"f":"json","username":current_Username,"password":current_Password,"client":"requestip"})
            tokenResponse = urllib.urlopen(current_AdminDirURL+"/generateToken?",tokenParams).read()
            tokenResponseJSON = json.loads(tokenResponse)
            token = tokenResponseJSON["token"]

            # Attempt to stop the current service
            startParams = urllib.urlencode({"token":token,"f":"json"})
            startResponse = urllib.urlopen(current_AdminDirURL+"/services/"+current_FolderName+"/"+current_ServiceName+"."+current_ServiceType+"/start?",startParams).read()
            startResponseJSON = json.loads(startResponse)
            startStatus = startResponseJSON["status"]

            if startStatus == "success":
                addToLog("PostETL_Do_Update_Service_And_CustomRasters: Started service "+str(current_FolderName)+"/"+str(current_ServiceName)+"/"+str(current_ServiceType))
            else:
                addToLog("PostETL_Do_Update_Service_And_CustomRasters: Unable to start service "+str(current_FolderName)+"/"+str(current_ServiceName)+"/"+str(current_ServiceType)+" STATUS = "+startStatus)
        except:
            e = sys.exc_info()[0]
            addToLog("PostETL_Do_Update_Service_And_CustomRasters: ERROR, Start Service failed for " + str(current_ServiceName) + ", System Error Message: "+ str(e))



def PostETL_Support_RemoveScratchFolders_Generic(folder):
    try:
        shutil.rmtree(folder)
    except:
        e = sys.exc_info()[0]
        addToLog("PostETL_Support_RemoveScratchFolders_Generic: ERROR: Error removing scratch folder "+str(folder)+" and its contents.  Please delete Manually.  System Error Message: " + str(e))

# Removes the Scratch Folders for etl processes.
def PostETL_Support_RemoveScratchFolders(pre, e, t, l, post):
    PostETL_Support_RemoveScratchFolders_Generic(pre)
    PostETL_Support_RemoveScratchFolders_Generic(e)
    PostETL_Support_RemoveScratchFolders_Generic(t)
    PostETL_Support_RemoveScratchFolders_Generic(l)
    PostETL_Support_RemoveScratchFolders_Generic(post)


def PostETL_Controller_Method(ETL_TransportObject):
    # Do a "PostETL" Process

    # Gathering inputs
    rasterOutputLocation = ETL_TransportObject['SettingsObj']['Raster_Final_Output_Location']
    intervalString = ETL_TransportObject['SettingsObj']['TRMM_RasterArchiveDays']
    regExp_Pattern = ETL_TransportObject['SettingsObj']['RegEx_DateFilterString']
    rastDateFormat = ETL_TransportObject['SettingsObj']['Python_DateFormat']
    theVarList = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Variable_Dictionary_List']
    oldDate = Unsorted_GetOldestDate(intervalString) #= ETL_TransportObject['Extract_Object']['ResultsObject']['OldestDateTime'] # oldDateTime = datetime.datetime.strptime("2014-05-03T12", "%Y-%m-%dT%H")

    GeoDB_Workspace = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['GeoDB_Dataset_Workspace']
    queryDateFormat = ETL_TransportObject['SettingsObj']['Query_DateFormat']
    # Inputs for the 3 Custom Raster generations.
    theOutputBasePath = GeoDB_Workspace #'C:\\kris\\!!Work\\ETL_TRMM\\GeoDB\\TRMM.gdb'
    PostETL_CustomRaster_Params = {
        'fileFolder_With_TRMM_Rasters' : ETL_TransportObject['SettingsObj']['Raster_Final_Output_Location'], # r'C:\ksArcPy\trmm\rastout',
        'color_map' : ETL_TransportObject['SettingsObj']['TRMM_ColorMapFile_3_Hour'],  # r'C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_3hour.clr',
        'output_basepath' : theOutputBasePath,
        'raster_catalog_fullpath' : theOutputBasePath + '\\' + theVarList[0]['mosaic_name'],  # \\TRMM', # Should be a setting  mosaic_name
        'raster_catalog_options_datetime_field' : theVarList[0]['primary_date_field'],  # 'timestamp',
        'raster_catalog_options_datetime_sql_cast' : 'date',
        'raster_catalog_options_datetime_field_format' : ETL_TransportObject['SettingsObj']['Query_DateFormat'],  # '%Y-%m-%d %H:00:00',
        'start_datetime' : datetime.datetime.utcnow(),
        'trmm1Day_RasterCatalogName' : ETL_TransportObject['SettingsObj']['trmm1Day_RasterCatalogName'],  #  'TRMM1Day',
        'trmm7Day_RasterCatalogName' : ETL_TransportObject['SettingsObj']['trmm7Day_RasterCatalogName'],  #  'TRMM7Day',
        'trmm30Day_RasterCatalogName' : ETL_TransportObject['SettingsObj']['trmm30Day_RasterCatalogName'],  #  'TRMM30Day',
        'trmm1Day_ColorMapLocation' : ETL_TransportObject['SettingsObj']['trmm1Day_ColorMapLocation'],  #  r'C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_1day.clr',
        'trmm7Day_ColorMapLocation' : ETL_TransportObject['SettingsObj']['trmm7Day_ColorMapLocation'],  #  r'C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_7day.clr',
        'trmm30Day_ColorMapLocation' : ETL_TransportObject['SettingsObj']['trmm30Day_ColorMapLocation'],  #  r'C:\kris\!!Work\ETL_TRMM\SupportFiles\TRMM_30Day.clr',
        'workSpacePath' : ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['PostETL'] # r'C:\kris\!!Work\ETL_TRMM\ScratchWorkspace\custom_RenameLater'

    }
    pkl_file = open('config.pkl', 'rb')
    myConfig = pickle.load(pkl_file)
    pkl_file.close()
    # service_Options_List
    Service_Options_30Min = [{
        "Description":"IMERG Mosaic Dataset Service",
        "admin_dir_URL":myConfig['admin_dir_URL'],
        "username":myConfig['username'],
        "password":myConfig['password'],
        "folder_name":myConfig['folder_name'],
        "service_name":"IMERG_30Min",
        "service_type":myConfig['service_type']
    }]
    # Update Service and Build new static composits.
    if len(ETL_TransportObject['Load_Object']['ResultsObject']['LoadResult_List']) == 0:
        addToLog("PostETL_Controller_Method: No items were loaded, Service will not be stopped and restarted.  Custom rasters will not be generated.")
    else:
        try:
            PostETL_Do_Update_Service_And_Custom_Rasters(PostETL_CustomRaster_Params, Service_Options_30Min, ETL_TransportObject)
            pass
        except:
            e = sys.exc_info()[0]
            addToLog("PostETL_Controller_Method: ERROR, something went wrong when trying to restart services and create the custom rasters.  System Error Message: "+ str(e))
    # Data Clean up
    if oldDate == None:
        # do nothing, we don't have an a date to use to remove items..
        pass
    else:
        # Data Clean up - Remove old raster items from the geodatabase
        num_Of_Rasters_Removed_FromGeoDB = Unsorted_removeRastersMosaicDataset(theVarList, GeoDB_Workspace, oldDate, queryDateFormat)

        # Data Clean up - Remove old rasters from the file system
        # Don't remove any rasters from the file system if the last step failed..
        addToLog("End of etl1")
		
        if num_Of_Rasters_Removed_FromGeoDB > 0:
            num_Of_Rasters_Deleted_FromFileSystem = Unsorted_dataCleanup(rasterOutputLocation, oldDate,regExp_Pattern,rastDateFormat)
    # Clean Scratch Workspaces
    folder_Pre = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['PreETL']
    folder_E = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['Extract']
    folder_T = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['Transform']
    folder_L = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['Load']
    folder_Post = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['PostETL']
    addToLog("PostETL_Controller_Method: Scratch Workspaces should now be cleaned")


    # Refresh the list of Permissions for accumulation rasters
    rasterDatasetList = ["IMERG1Day","IMERG3Day", "IMERG7Day", "IMERG30Day"]
    addToLog("End of etl2")
	
    #PostETL_RefreshPermissions_For_Accumulations(GeoDB_Workspace, rasterDatasetList)
    IsError = False
    ErrorMessage = ""
    # Package up items from the PreETL Step
    returnObj = {

        'IsError': IsError,
        'ErrorMessage':ErrorMessage
    }
    addToLog("End of etl3")
    # Return the packaged items.
    return returnObj


    return "PostETL_Controller_Method result object here"


#--------------------------------------------------------------------------
# Finalized Simple Log Report
#   This function checks for errors and various metrics of the ETL
#   process and outputs all that to the log near the end of code execution
#--------------------------------------------------------------------------

# Check for error, output the message if found.
def output_Error_For_ResultObj(resultObj, sectionName):
    try:
        if resultObj['IsError'] == True:
            errMsg = resultObj['ErrorMessage']
            addToLog(" === ERROR REPORT: "+str(sectionName)+":  " + str(errMsg))
        else:
            addToLog(" === REPORT: "+str(sectionName)+":  No errors to report.")
    except:
        e = sys.exc_info()[0]
        addToLog("output_Error_For_ResultObj: ERROR: Error displaying errors for: "+str(sectionName)+" System Error Message: " + str(e))



def output_Final_Log_Report(ETL_TransportObject):

    # Get report items and output them.

    # Extract
    try:
        numExtracted = len(ETL_TransportObject['Extract_Object']['ResultsObject']['ExtractResult']['ExtractList'])
        addToLog(" === REPORT: Extract: " + str(numExtracted) + " Items were extracted.")
    except:
        e = sys.exc_info()[0]
        addToLog("output_Final_Log_Report: ERROR: Error outputing Extract report.  System Error Message: " + str(e))

    # Transform
    try:
        numTransformed = len(ETL_TransportObject['Transform_Object']['ResultsObject']['TransformResult_List'])
        addToLog(" === REPORT: Transform: " + str(numTransformed) + " Items were transformed.")
    except:
        e = sys.exc_info()[0]
        addToLog("output_Final_Log_Report: ERROR: Error outputing Transform report.  System Error Message: " + str(e))

    # Load
    try:
        numLoaded = len(ETL_TransportObject['Load_Object']['ResultsObject']['LoadResult_List'])
        addToLog(" === REPORT: Load: " + str(numLoaded) + " Items were loaded.")
    except:
        e = sys.exc_info()[0]
        addToLog("output_Final_Log_Report: ERROR: Error outputing Load report.  System Error Message: " + str(e))


    # Errors for each step
    resultsObj_PreETL = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']
    resultsObj_Extract = ETL_TransportObject['Extract_Object']['ResultsObject']
    resultsObj_Transform = ETL_TransportObject['Transform_Object']['ResultsObject']
    resultsObj_Load = ETL_TransportObject['Load_Object']['ResultsObject']
    resultsObj_PostETL = ETL_TransportObject['Post_ETL_Object']['ResultsObject']
    output_Error_For_ResultObj(resultsObj_PreETL, "Pre ETL")
    output_Error_For_ResultObj(resultsObj_Extract, "Extract")
    output_Error_For_ResultObj(resultsObj_Transform, "Transform")
    output_Error_For_ResultObj(resultsObj_Load, "Load")
    output_Error_For_ResultObj(resultsObj_PostETL, "Post ETL")

#--------------------------------------------------------------------------
# Controller
#--------------------------------------------------------------------------
def main(config_Settings):

    # Get a start time for the entire script run process.
    time_TotalScriptRun_Process = get_NewStart_Time()

    # Clear way to show entry in the log file for a script session start
    addToLog("======================= SESSION START =======================")

    # Config Settings
    settingsObj = config_Settings.xmldict['ConfigObjectCollection']['ConfigObject']

    # Access to the Config settings example
    current_ScriptSession_Name =  settingsObj['Name']
    addToLog("Script Session Name is: " + current_ScriptSession_Name)

    # Set up Detailed Logging
    current_DetailedLogging_Setting = settingsObj['DetailedLogging']
    global g_DetailedLogging_Setting
    if current_DetailedLogging_Setting == '1':
        g_DetailedLogging_Setting = True
    else:
        g_DetailedLogging_Setting = False
    addToLog("Main: Detailed logging has been enabled", True)


    # Prep Objects
    VariableDictionaryList = settingsObj['VariableDictionaryList']

    # Create ETL_TransportObject
    ETL_TransportObject = {
        "SettingsObj": settingsObj,
        "Pre_ETL_Object" : {"ResultsObject":None,"OtherItems":None},
        "Extract_Object" : {"ResultsObject":None,"OtherItems":None},
        "Transform_Object" : {"ResultsObject":None,"OtherItems":None},
        "Load_Object" : {"ResultsObject":None,"OtherItems":None},
        "Post_ETL_Object" : {"ResultsObject":None,"OtherItems":None},
    }

    # Detailed log entry showing the current state of the ETL_TransportObject
    addToLog("main: Current State of ETL_TransportObject (Before PreETL method call): " + str(ETL_TransportObject), True)
	
    # Execute Pre ETL, Log the Time, and load the Results object.
    time_PreETL_Process = get_NewStart_Time()
    try:
        addToLog("========= Pre ETL =========")
        ETL_TransportObject['Pre_ETL_Object']['ResultsObject'] = PreETL_Controller_Method(ETL_TransportObject)
    except:
        e = sys.exc_info()[0]
        addToLog("main: PreETL ERROR, something went wrong, ERROR MESSAGE: "+ str(e))
    addToLog("TIME PERFORMANCE: time_PreETL_Process : " + get_Elapsed_Time_As_String(time_PreETL_Process))
    # Detailed log entry showing the current state of the ETL_TransportObject
    addToLog("main: Current State of ETL_TransportObject (Before Extract method call): " + str(ETL_TransportObject), True)

    # Execute Extract, Log the Time, and load the Results object.
    time_Extract_Process = get_NewStart_Time()
    try:
        addToLog("========= EXTRACTING =========")
        ETL_TransportObject['Extract_Object']['ResultsObject'] = Extract_Controller_Method(ETL_TransportObject)
    except:
        e = sys.exc_info()[0]
        addToLog("main: EXTRACTING ERROR, something went wrong, ERROR MESSAGE: "+ str(e))
    addToLog("TIME PERFORMANCE: time_Extract_Process : " + get_Elapsed_Time_As_String(time_Extract_Process))
    # Detailed log entry showing the current state of the ETL_TransportObject
    addToLog("main: Current State of ETL_TransportObject (Before Transform method call): " + str(ETL_TransportObject), True)

    # Execute Transform, Log the Time, and load the Results object.
    time_Transform_Process = get_NewStart_Time()
    try:
        addToLog("========= TRANSFORMING =========")
        ETL_TransportObject['Transform_Object']['ResultsObject'] = Transform_Controller_Method(ETL_TransportObject)
    except:
        e = sys.exc_info()[0]
        addToLog("main: TRANSFORMING ERROR, something went wrong, ERROR MESSAGE: "+ str(e))
    addToLog("TIME PERFORMANCE: time_Transform_Process : " + get_Elapsed_Time_As_String(time_Transform_Process))
    # Detailed log entry showing the current state of the ETL_TransportObject
    addToLog("main: Current State of ETL_TransportObject (Before Load method call): " + str(ETL_TransportObject), True)

    # Execute Load, Log the Time, and load the Results object.
    time_Load_Process = get_NewStart_Time()
    try:
        addToLog("========= LOADING =========")
        ETL_TransportObject['Load_Object']['ResultsObject'] = Load_Controller_Method(ETL_TransportObject)
        try:
    		
			pkl_file = open('config.pkl', 'rb')
    		
			myConfig = pickle.load(pkl_file)
    		
			pkl_file.close()
    		
			mosaicDSPath = myConfig['mosaicDatasetPath'] 
			mosaicDS = myConfig['DSNAME']     		
			arcpy.CalculateStatistics_management(in_raster_dataset=mosaicDSPath+'/'+mosaicDS, x_skip_factor="1", y_skip_factor="1", ignore_values="", skip_existing="OVERWRITE", area_of_interest="Feature Set")
			addToLog("Load_Do_Load_Dataset: Calculated statistics on mosaic dataset "+str(mosaicDS))		
			arcpy.Compact_management(in_workspace=mosaicDSPath)
			addToLog("Load_Do_Load: compacted file GDB ")
			pass

			# Handle errors for calc statistics
        except:
			e = sys.exc_info()[0]
			addToLog("Load_Dataset: ERROR: Error calculating statistics on mosaic dataset Error Message: "+str(e) + " ArcPy Messages: " + str(arcpy.GetMessages(2)))
			pass		
    except:
        e = sys.exc_info()[0]
        addToLog("main: LOADING ERROR, something went wrong, ERROR MESSAGE: "+ str(e))
    addToLog("TIME PERFORMANCE: time_Load_Process : " + get_Elapsed_Time_As_String(time_Load_Process))
    # Detailed log entry showing the current state of the ETL_TransportObject
    addToLog("main: Current State of ETL_TransportObject (Before PostETL method call): " + str(ETL_TransportObject), True)

    # Execute Post ETL, Log the Time, and load the Results object.
    time_PostETL_Process = get_NewStart_Time()
    try:
        addToLog("========= Post ETL =========")
        ETL_TransportObject['Post_ETL_Object']['ResultsObject'] = PostETL_Controller_Method(ETL_TransportObject)
    except:
        e = sys.exc_info()[0]
        addToLog("main: Post ETL ERROR, something went wrong, ERROR MESSAGE: "+ str(e))
    addToLog("TIME PERFORMANCE: time_PostETL_Process : " + get_Elapsed_Time_As_String(time_PostETL_Process))
    # Detailed log entry showing the current state of the ETL_TransportObject
    addToLog("main: Current State of ETL_TransportObject (After PostETL method call): " + str(ETL_TransportObject), True)

    # Echo Errors to the log
    try:
        output_Final_Log_Report(ETL_TransportObject)
    except:
        e = sys.exc_info()[0]
        addToLog("main: Error Reporting Errors!, something went wrong, ERROR MESSAGE: "+ str(e))

    # Add a log entry showing the amount of time the script ran.
    # Note: It may be good practice to use a common phrase such as "TIME PERFORMANCE" to make it easier to search log files for the performance details since the log files can end up generating a lot of text.
    addToLog("TIME PERFORMANCE: time_TotalScriptRun_Process : " + get_Elapsed_Time_As_String(time_TotalScriptRun_Process))

    # Clear way to show entry in the log file for a script session end
    addToLog("======================= SESSION END =======================")
    # Add a few lines so we can tell sessions apart in the log more quickly
    addToLog("===")
    addToLog("===")
    addToLog("===")
    addToLog("===")
    # END
# Entry Point
main(g_ConfigSettings)

# END