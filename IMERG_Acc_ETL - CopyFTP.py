#-------------------------------------------------------------------------------
# Name:        IMERG_ETL.py
# Purpose:     SERVIR Implementation of IMERG ETL Scripts for various ArcGIS products and services.
# Author:      Kris Stanton
# Last Modified By: Githika Tondapu Jun 30,2017
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
import ssl
# SD's files: "arcpy_utils.py" and "etl_utls"
from arcpy_utils import FileGeoDatabase, RasterCatalog, AGServiceManager
from etl_utils import FTPDownloadManager

# SD's file for creating static maps.
from arcpy_trmm_custom_raster import TRMMCustomRasterRequest, TRMMCustomRasterCreator
from copy import deepcopy

from time import mktime
from datetime import datetime
from datetime import timedelta

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
        oldestDate = datetime.utcnow() - timedelta(days=intervalValue) #datetime.timedelta(**deltaArgs)
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
            tempDateTime = datetime.strptime(rastDateStr, rastDateFormat)
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


#--------------------------------------------------------------------------
# Post ETL
#   Processes that must be performed after the ETL process.
#   This may also include operations on the data which are independent of the
#   ETL process.  For example, CREST's insert line items to seperate postgres
#   DB operations.
#--------------------------------------------------------------------------


# Refresh the list of Permissions
def PostETL_RefreshPermissions_For_Accumulations(pathToGeoDB, rasterDatasetList):
    for dataSetName in rasterDatasetList:
        try:
            mds = os.path.join(pathToGeoDB, dataSetName)
            addToLog(mds + "*************MDS*****************")
            arcpy.ChangePrivileges_management(mds,"role_servir_editor","GRANT","#")
            addToLog("PostETL_RefreshPermissions_For_Accumulations: Editor Permissions set for " + str(dataSetName) + ", arcpy Message: " + str(arcpy.GetMessages()))
            arcpy.ChangePrivileges_management(mds,"role_servir_viewer","GRANT","#")
            addToLog("PostETL_RefreshPermissions_For_Accumulations: Viewer Permissions set for " + str(dataSetName) + ", arcpy Message: " + str(arcpy.GetMessages()))
        except:
            e = sys.exc_info()[0]
            addToLog("PostETL_RefreshPermissions_For_Accumulations: ERROR, Something went wrong when setting permissions.  System Error Message: "+ str(e) + ", ArcPy Message: " + str(arcpy.GetMessages()))



# SPECIAL SECTION.. UPDATE LABELS ON A SHAPEFILE WHICH DISPLAY THE RANGE FOR ACCUMULATIONS
# SPECIAL SECTION.. UPDATE LABELS ON A SHAPEFILE WHICH DISPLAY THE RANGE FOR ACCUMULATIONS
# SPECIAL SECTION.. UPDATE LABELS ON A SHAPEFILE WHICH DISPLAY THE RANGE FOR ACCUMULATIONS

# Get the SHP Label Message string from a current date time and a number of days
def Update_Accumulations_Get_SHP_Label_Message(fileDateTime, numOfDays):
    try:
        # Create delta time param from number of days
        intervalType = 'days'
        intervalValue = int(str(numOfDays))
        deltaArgs = {intervalType:intervalValue}
        earliestDateTime = fileDateTime - timedelta(days=intervalValue)
       # earliestDateTime = fileDateTime - datetime.timedelta(**deltaArgs)
		
        latestDateTime = fileDateTime
        # String format is 'earliestDateTime - latestDateTime'
        outDateTime_FormatStr = "%Y-%m-%d %H:%M"
        stringPart_1 = earliestDateTime.strftime(outDateTime_FormatStr)
        stringPart_2 = " UTC - "
        stringPart_3 = latestDateTime.strftime(outDateTime_FormatStr)
        stringPart_4 = " UTC"
        retString = stringPart_1 + stringPart_2 + stringPart_3 + stringPart_4

        # Output should look something like this
        return retString
    except:
        e = sys.exc_info()[0]
        addToLog("Update_Accumulations_Get_SHP_Label_Message: ERROR, Something went wrong when Getting the label message  System Error Message: "+ str(e)) # + ", ArcPy Message: " + str(arcpy.GetMessages()))
        return None

# Expected base filename format example "3B-HHR-L.MS.MRG.3IMERG.20150416-S090000-E092959.0540.V03E.7day"
def Update_Accumulations_Get_DateTime_From_BaseFileName(baseFileName):
    try:
        # convert filename to datetime object which represents this particular file
        datetimePart = baseFileName.split(".")[4] # ex '20150416-S090000-E092959'
        datePart = datetimePart.split("-")[0] # ex '20150416'
        startTimeCode = datetimePart.split('-')[1] # ex 'S090000'
        hoursPart = startTimeCode[1:3]  # ex '09'
		
        minutesPart = startTimeCode[3:5] # ex '00'
		
        formatString = "%Y%m%d%H%M"
        datetimeString = datePart + hoursPart + minutesPart # format 'yyyymmddHHMM' # ex '201504160900
		
        currentFilenameDateTime = datetime.strptime(datetimeString, formatString)
		
        return currentFilenameDateTime
    except:
        e = sys.exc_info()[0]
        addToLog("Update_Accumulations_Get_DateTime_From_BaseFileName: ERROR, Something went wrong when Getting the datetime from the baseFileName, " + str(baseFileName) + ",  System Error Message: "+ str(e)) # + ", ArcPy Message: " + str(arcpy.GetMessages()))
        return None

# Update the shape file's data field.
def Update_Accumulations_Label_Do_Update(shpFile, labelTextField, newTextValue):
    try:
        with arcpy.da.UpdateCursor(shpFile, labelTextField) as cursor:
            for row in cursor:
                row[0] = newTextValue
                cursor.updateRow(row)

    except:
        e = sys.exc_info()[0]
        addToLog("Update_Accumulations_Label_Do_Update: ERROR, Something went wrong when Updating the shp file, " + str(shpFile) + ",  System Error Message: "+ str(e) + ", ArcPy Message: " + str(arcpy.GetMessages()))
        pass

# Controller function which pulls it all together to make a text update (All functions with prefix ' Update_Accumulations_Label_ ' are controlled by this function.
def Update_Accumulations_Label_Controller(shapeFilePath, labelTextField, numOfDays, theFileName):
    try:
        # Get the File's DateTime (Parse the filename to extract a datetime object from it.)
        fileDateTime = Update_Accumulations_Get_DateTime_From_BaseFileName(theFileName)

        # Get the message from the file's datetime object and number of days
        theMsg = Update_Accumulations_Get_SHP_Label_Message(fileDateTime, numOfDays)

        # Update the message
        Update_Accumulations_Label_Do_Update(shapeFilePath, labelTextField, theMsg)

        addToLog("Update_Accumulations_Label_Controller: Accumulation Label has updated with message: " + str(theMsg))
    except:
        e = sys.exc_info()[0]
        addToLog("Update_Accumulations_Label_Controller: ERROR, Something went wrong when Updating the accumulation label.  System Error Message: "+ str(e) + ", ArcPy Message: " + str(arcpy.GetMessages()))


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

def Download_And_Load_CustomRaster(lastRasterName, whichComposite, ftpSubfolder, ftpParams, scratchFolder, coor_system, pathToGeoDB, rasterDataSetName):
    # filter input
    if whichComposite == "1day":
        pass
    elif whichComposite == "3day":
        pass
    elif whichComposite == "7day":
        pass
    else:
        addToLog("Download_And_Load_CustomRaster: Bad input value for 'whichComposite' : " + str(whichComposite) + ", bailing out!")
        return

    # KS Refactor For 30 Min Datasets // Need to replace the 30min part instead of the 3hr
    newBaseName = lastRasterName.replace("30min", whichComposite)
    TIF_FileName = newBaseName + ".tif"
    TFW_FileName = newBaseName + ".tfw"
    location_ToSave_TIF_File = os.path.join(scratchFolder,TIF_FileName)
    location_ToSave_TFW_File = os.path.join(scratchFolder,TFW_FileName)
    subTransformScratchFolder = os.path.join(scratchFolder,whichComposite)
    trans_Raster_File = os.path.join(subTransformScratchFolder,TIF_FileName)
    # Create Temp Subfolder
    try:
        make_And_Validate_Folder(subTransformScratchFolder)
    except:
        e = sys.exc_info()[0]
        addToLog("Download_And_Load_CustomRaster: ERROR, Something went wrong when creating the sub scratch folder.  System Error Message: "+ str(e))

    #addToLog("CUSTOM RASTERS SUB:  Alert L2.... Created (or tried to create) the subfolder.. ")

    # Connect to FTP, download the files  # TRMMs ftp acts funny if we don't enter delays.. thats why using time.sleep(1)
    # ftpParams : ftpHost, ftpUserName, ftpUserPass
    time.sleep(1)
    ftp_Connection = ftplib.FTP(ftpParams['ftpHost'],ftpParams['ftpUserName'],ftpParams['ftpUserPass'])
    time.sleep(1)

    # Change Folder FTP
    # Extra ftpSubfolder
    ftp_Connection.cwd(ftpSubfolder)
    time.sleep(1)
    # Download the TIF and World Files
   #Githika with open(location_ToSave_TIF_File, "wb") as f:
    #    ftp_Connection.retrbinary("RETR %s" % TIF_FileName, f.write)

    #time.sleep(1)

    # Log the datetime of the raster accumulation
    #addToLog("Download_And_Load_CustomRaster: Downloaded Accumulation Raster: " + str(TIF_FileName))


    #with open(location_ToSave_TFW_File, "wb") as f:
    #    ftp_Connection.retrbinary("RETR %s" % TFW_FileName, f.write)
    #time.sleep(1)
#Githika

    try:
		location_ToSave_TIF_File = os.path.join(scratchFolder,TIF_FileName)
		with open(location_ToSave_TIF_File, "wb") as f:		
			ftp_Connection.retrbinary("RETR %s" % TIF_FileName, f.write)
			time.sleep(1)
    except:
		os.remove(os.path.join(scratchFolder,TIF_FileName))

		TIF_FileName = TIF_FileName.replace("05B", "04B")
		location_ToSave_TIF_File = os.path.join(scratchFolder,TIF_FileName)		
		try:							
			with open(location_ToSave_TIF_File, "wb") as f:		
				ftp_Connection.retrbinary("RETR %s" % TIF_FileName, f.write)		
				time.sleep(1)
		except:
			os.remove(os.path.join(scratchFolder,TIF_FileName))

			TIF_FileName = TIF_FileName.replace("04B", "04A")
			location_ToSave_TIF_File = os.path.join(scratchFolder,TIF_FileName)
			try:							
				with open(location_ToSave_TIF_File, "wb") as f:			
					ftp_Connection.retrbinary("RETR %s" % TIF_FileName, f.write)
					time.sleep(1)	
			except:
				os.remove(os.path.join(scratchFolder,TIF_FileName))

				TIF_FileName = TIF_FileName.replace("04A", "03E")
				location_ToSave_TIF_File = os.path.join(scratchFolder,TIF_FileName)
				try:							
					with open(location_ToSave_TIF_File, "wb") as f:			
						ftp_Connection.retrbinary("RETR %s" % TIF_FileName, f.write)
						time.sleep(1)	
				except:
					addToLog("",True)											
									
									
    try:
		location_ToSave_TFW_File = os.path.join(scratchFolder,TFW_FileName)
		with open(location_ToSave_TFW_File, "wb") as f:								
			ftp_Connection.retrbinary("RETR %s" % TFW_FileName, f.write)
			time.sleep(1)
    except:		
		os.remove(os.path.join(scratchFolder,TFW_FileName))
		TFW_FileName = TFW_FileName.replace("05B", "04B")
		location_ToSave_TFW_File = os.path.join(scratchFolder,TFW_FileName)
		try:														
			with open(location_ToSave_TFW_File, "wb") as f:								
				ftp_Connection.retrbinary("RETR %s" % TFW_FileName, f.write)
				time.sleep(1)
		except:
			os.remove(os.path.join(scratchFolder,TFW_FileName))
			TFW_FileName = TFW_FileName.replace("04B", "04A")
			location_ToSave_TFW_File = os.path.join(scratchFolder,TFW_FileName)	
			try:														
				with open(location_ToSave_TFW_File, "wb") as f:								
					ftp_Connection.retrbinary("RETR %s" % TFW_FileName, f.write)
					time.sleep(1)	
			except:
				os.remove(os.path.join(scratchFolder,TFW_FileName))
				TFW_FileName = TFW_FileName.replace("04A", "03E")
				location_ToSave_TFW_File = os.path.join(scratchFolder,TFW_FileName)	
				try:														
					with open(location_ToSave_TFW_File, "wb") as f:								
						ftp_Connection.retrbinary("RETR %s" % TFW_FileName, f.write)
						time.sleep(1)
				except:
					addToLog("",True)							
    ftp_Connection.close()

    # Apply Transform (Spatial Projection) # Apply the projection BEFORE copying the raster!
    sr = arcpy.SpatialReference(coor_system)

    arcpy.DefineProjection_management(location_ToSave_TIF_File, sr)
    addToLog("Download_And_Load_CustomRaster: Spatial Projection, " + str(sr.name) + " applied to raster: " + str(location_ToSave_TIF_File))

    # Calculate Statistics
    arcpy.CalculateStatistics_management(location_ToSave_TIF_File,1,1,"#","SKIP_EXISTING","#")
    addToLog("Download_And_Load_CustomRaster:  CalculateStatistics_management Finished")

    # Copy Rasters from temp location to final location
    addToLog("Download_And_Load_CustomRaster:  About to copy (refresh) File System Accumulation Raster: " + str(TIF_FileName))

    #location_ToSave_TIF_File = os.path.join(scratchFolder,TIF_FileName)
    source_FolderPath = scratchFolder                       # ex       Z:\\ETLscratch\\IMERG\\PostETL
    source_FileName = TIF_FileName                          # ex       3B-HHR-L.MS.MRG.3IMERG.20150416-S090000-E092959.0540.V03E.7day.tif
    source_BaseFileName = source_FileName[:-4]              # ex       3B-HHR-L.MS.MRG.3IMERG.20150416-S090000-E092959.0540.V03E.7day     #os.path.basename(TIF_FileName)
    source_FileExtensionStrings = ["tif", "tfw", "tif.aux.xml", "tif.xml"]
    dest_FolderPath = pathToGeoDB
    dest_BaseFileName = rasterDataSetName

    # Loop to copy all files!
    for currentExtension in source_FileExtensionStrings:

        # Get the source full path
        current_SourceFileName = source_BaseFileName + "." + currentExtension
        current_SourceFullFilePath = os.path.join(source_FolderPath, current_SourceFileName)

        # Get the Destination full path
        current_DestFileName = dest_BaseFileName + "." + currentExtension
        current_DestFullFilePath = os.path.join(dest_FolderPath, current_DestFileName)

        # Delete the files that already exist in that location
        try:
            os.remove(current_DestFullFilePath)
            addToLog("Download_And_Load_CustomRaster: Removed file: " + str(current_DestFullFilePath))
            pass
        except:
            e = sys.exc_info()[0]
            addToLog("Download_And_Load_CustomRaster: ERROR: Error Removing file: " + str(current_DestFullFilePath) + " Error Message: " + str(e)) # + " ArcPy Error Message: " + str(arcpy.GetMessages()))
            pass

        # Copy function goes here!!
        try:
            shutil.copy(current_SourceFullFilePath, current_DestFullFilePath)
            addToLog("Download_And_Load_CustomRaster: Copied file FROM: " + str(current_SourceFullFilePath) + ", TO: " + str(current_DestFullFilePath))
            pass
        except:
            e = sys.exc_info()[0]
            addToLog("Download_And_Load_CustomRaster: ERROR: Error copying file: " + str(current_SourceFullFilePath) + " Error Message: " + str(e)) # + " ArcPy Error Message: " + str(arcpy.GetMessages()))
            pass

    # Refactor Cleanup
    try:
        # old files left over from previous runs
        HC_File_1 = r'E:\SERVIR\Data\Global\IMERG\IMERG1Day.tif.ovr'
        HC_File_3 = r'E:\SERVIR\Data\Global\IMERG\IMERG3Day.tif.ovr'
        HC_File_7 = r'E:\SERVIR\Data\Global\IMERG\IMERG7Day.tif.ovr'
        try:
            os.remove(HC_File_1)
        except:
            pass
        try:
            os.remove(HC_File_3)
        except:
            pass
        try:
            os.remove(HC_File_7)
        except:
            pass
    except:
        pass

    # Update Labels on Shp Files
    try:
       
        # Hard Coded Inputs (TODO!! When time permits, CHANGE THESE TO SETTINGS)
        HC_Label_TextField = 'RangeLBL'
        HC_PathTo_LabelSHPFile_1_Day = r'E:\SERVIR\Data\Global\IMERG\AccLabels\AccumulationLabelFeat_1Day.shp'
        HC_PathTo_LabelSHPFile_3_Day = r'E:\SERVIR\Data\Global\IMERG\AccLabels\AccumulationLabelFeat_3Day.shp'
        HC_PathTo_LabelSHPFile_7_Day = r'E:\SERVIR\Data\Global\IMERG\AccLabels\AccumulationLabelFeat_7Day.shp'


        # Fixed inputs (fixed no matter which accumulation is being updated)
        labelTextField = HC_Label_TextField # ex "RangeLBL"
        theFileName = source_BaseFileName # None # "3B-HHR-L.MS.MRG.3IMERG.20150416-S090000-E092959.0540.V03E.7day"

        # Variable Inputs (varies based on which accumulation is being updated)
        shapeFilePath = None # r'E:\SERVIR\Data\Global\IMERG\AccLabels\AccumulationLabelFeat_7Day.shp'
        numOfDays = None # ex 7


        # rasterDataSetName # ex IMERG1Day, or IMERG3Day, or IMERG7Day
        # Set the Varabile inputs and call the functions to update the labels
        if rasterDataSetName == "IMERG1Day":
            shapeFilePath = HC_PathTo_LabelSHPFile_1_Day
            numOfDays = 1
            Update_Accumulations_Label_Controller(shapeFilePath, labelTextField, numOfDays, theFileName)
            addToLog("Download_And_Load_CustomRaster: Label for Raster, " + str(rasterDataSetName) + " has been updated.")
        elif rasterDataSetName == "IMERG3Day":
            shapeFilePath = HC_PathTo_LabelSHPFile_3_Day
            numOfDays = 3
            Update_Accumulations_Label_Controller(shapeFilePath, labelTextField, numOfDays, theFileName)
            addToLog("Download_And_Load_CustomRaster: Label for Raster, " + str(rasterDataSetName) + " has been updated.")
        elif rasterDataSetName == "IMERG7Day":
            shapeFilePath = HC_PathTo_LabelSHPFile_7_Day
            numOfDays = 7
            Update_Accumulations_Label_Controller(shapeFilePath, labelTextField, numOfDays, theFileName)
            addToLog("Download_And_Load_CustomRaster: Label for Raster, " + str(rasterDataSetName) + " has been updated.")
        else:
            addToLog("Download_And_Load_CustomRaster: Input params for updating the label were invalid.  Label for Raster, " + str(rasterDataSetName) + ", has NOT been updated.")

        pass
    except:
        e = sys.exc_info()[0]
        addToLog("Download_And_Load_CustomRaster: ERROR, Something went wrong when Updating the accumulation label for Raster, " + str(rasterDataSetName) + "  Label has NOT been updated.")

        pass

def PostETL_Support_Build_Custom_Rasters(PostETL_CustomRaster_Params):
    # Gather params
    fileFolder_With_TRMM_Rasters = PostETL_CustomRaster_Params['fileFolder_With_TRMM_Rasters'] # r"C:\ksArcPy\trmm\rastout" # Settings, 'Raster_Final_Output_Location'
    color_map = PostETL_CustomRaster_Params['color_map'] # r"C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_3hour.clr" # PLACEHOLDER
    output_basepath = PostETL_CustomRaster_Params['output_basepath'] # "C:\\kris\\!!Work\\ETL_TRMM\\GeoDB\\TRMM.gdb"
    raster_catalog_fullpath = PostETL_CustomRaster_Params['raster_catalog_fullpath'] # output_basepath + "\\TRMM"
    raster_catalog_options_datetime_field = PostETL_CustomRaster_Params['raster_catalog_options_datetime_field'] # "timestamp"  #"datetime"
    raster_catalog_options_datetime_sql_cast = PostETL_CustomRaster_Params['raster_catalog_options_datetime_sql_cast'] # "date"
    raster_catalog_options_datetime_field_format = PostETL_CustomRaster_Params['raster_catalog_options_datetime_field_format'] # "%Y-%m-%d %H:00:00" # Query_DateFormat>%Y-%m-%d %H:00:00 # "%m-%d-%Y %I:%M:%S %p"
    start_datetime = PostETL_CustomRaster_Params['start_datetime'] # datetime.utcnow()
    trmm1Day_RasterCatalogName = PostETL_CustomRaster_Params['trmm1Day_RasterCatalogName'] # "TRMM1Day"
    trmm7Day_RasterCatalogName = PostETL_CustomRaster_Params['trmm7Day_RasterCatalogName'] # "TRMM7Day"
    trmm30Day_RasterCatalogName = PostETL_CustomRaster_Params['trmm30Day_RasterCatalogName'] # "TRMM30Day"
    trmm1Day_ColorMapLocation = PostETL_CustomRaster_Params['trmm1Day_ColorMapLocation'] # r"C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_1day.clr"
    trmm7Day_ColorMapLocation = PostETL_CustomRaster_Params['trmm7Day_ColorMapLocation'] # r"C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_7day.clr"
    trmm30Day_ColorMapLocation = PostETL_CustomRaster_Params['trmm30Day_ColorMapLocation'] # r"C:\kris\!!Work\ETL_TRMM\SupportFiles\TRMM_30Day.clr"
    workSpacePath = PostETL_CustomRaster_Params['workSpacePath'] # r"C:\kris\!!Work\ETL_TRMM\ScratchWorkspace\custom_RenameLater"
    # 'clip_extent' <str>: the processing extent contained within "-180.0 -50.0 180.0 50.0"
    # initialize request config objects -------------------------------------

    factory_specifications = {

        "AddColormap_management_config": { # optional, comment out/delete entire key if no color map is needed
            "input_CLR_file":color_map
        },
        "CopyRaster_management_config":{
            'config_keyword':'',
            'background_value':'',
            'nodata_value':'',
            'onebit_to_eightbit':'',
            'colormap_to_RGB':'',
            'pixel_type':'32_BIT_UNSIGNED'#'16_BIT_UNSIGNED'
        }
    }

    input_raster_catalog_options = {

        'raster_catalog_fullpath': raster_catalog_fullpath,  # raster_catalog.fullpath,
        "raster_name_field":'Name',
        "datetime_field":raster_catalog_options_datetime_field,                 #raster_catalog.options['datetime_field'],                  # Original Val "datetime"
        'datetime_sql_cast':raster_catalog_options_datetime_sql_cast,           # raster_catalog.options['datetime_sql_cast'],              # Original Val "date"
        'datetime_field_format':raster_catalog_options_datetime_field_format,    # raster_catalog.options['datetime_field_format'],          # Original Val "%m-%d-%Y %I:%M:%S %p"
        'start_datetime':start_datetime
    }

    # TRMM1Day config --------------------------------------------------------------------------------
    factory_specifications_1day = deepcopy(factory_specifications)
    factory_specifications_1day['output_raster_fullpath'] = os.path.join(output_basepath, trmm1Day_RasterCatalogName) #"TRMM1Day")
    factory_specifications_1day['AddColormap_management_config']['input_CLR_file'] = trmm1Day_ColorMapLocation # "D:\\SERVIR\\ReferenceNode\\MapServices\\trmm_1day.clr"
    input_raster_catalog_options_1day = deepcopy(input_raster_catalog_options)
    input_raster_catalog_options_1day['end_datetime'] = start_datetime - timedelta(days=1)
    trmm_1day = TRMMCustomRasterRequest({

        'debug_logger':addToLog,
        'factory_specifications':factory_specifications_1day,
        'input_raster_catalog_options':input_raster_catalog_options_1day
    })

    # TRMM7Day config --------------------------------------------------------------------------------
    factory_specifications_7day = deepcopy(factory_specifications)
    factory_specifications_7day['output_raster_fullpath'] = os.path.join(output_basepath, trmm7Day_RasterCatalogName) #"TRMM7Day")
    factory_specifications_7day['AddColormap_management_config']['input_CLR_file'] = trmm7Day_ColorMapLocation #"D:\\SERVIR\\ReferenceNode\\MapServices\\trmm_7day.clr"
    input_raster_catalog_options_7day = deepcopy(input_raster_catalog_options)
    input_raster_catalog_options_7day['end_datetime'] = start_datetime - timedelta(days=7)
    trmm_7day = TRMMCustomRasterRequest({

        'debug_logger':addToLog,
        'factory_specifications':factory_specifications_7day,
        'input_raster_catalog_options':input_raster_catalog_options_7day
    })

    # TRMM30Day config --------------------------------------------------------------------------------
    factory_specifications_30day = deepcopy(factory_specifications)
    factory_specifications_30day['output_raster_fullpath'] = os.path.join(output_basepath, trmm30Day_RasterCatalogName) #"TRMM30Day")
    factory_specifications_30day['AddColormap_management_config']['input_CLR_file'] = trmm30Day_ColorMapLocation #"D:\\SERVIR\\ReferenceNode\\MapServices\\TRMM_30Day.clr"
    input_raster_catalog_options_30day = deepcopy(input_raster_catalog_options)
    input_raster_catalog_options_30day['end_datetime'] = start_datetime - timedelta(days=30)
    trmm_30day = TRMMCustomRasterRequest({

        'debug_logger':addToLog,
        'factory_specifications':factory_specifications_30day,
        'input_raster_catalog_options':input_raster_catalog_options_30day
    })

    # initialize object responsible for creating the TRMM composities
    trmm_custom_raster_factory = TRMMCustomRasterCreator({

        'workspace_fullpath': workSpacePath, 
        'remove_all_rasters_on_finish':False,
        'archive_options': {
            'raster_name_prefix':"t_", # identify rasters to delete by this prefix
            'local_raster_archive_days':30, # only keep rasters local within this many days
            'raster_name_datetime_format':"t_%Y%m%d%H" # format of rasters to create a datetime object
        },
        'fileFolder_With_TRMM_Rasters' : fileFolder_With_TRMM_Rasters,
        'debug_logger':addToLog,
        'exception_handler':addToLog #exception_manager.handleException
    })


    # And for the 1, 3, and 7 day.. download them from the source and upload them.
    try:
        # FTP Info
        pkl_file = open('config.pkl', 'r')
        myConfig = pickle.load(pkl_file) #store the data from config.pkl file
        pkl_file.close()
        ftpParams = {
            "ftpHost" : myConfig['ftp_host'], 
            "ftpUserName" : myConfig['ftp_user'], 
            "ftpUserPass" : myConfig['ftp_pswrd'] 
        }
        today = str(datetime.today());
        curr_year = today[:4];
        curr_month = today[5:7];
        curr_date=today[8:10];
        curr_hour=today[11:13];
        curr_minutes=today[14:16];
        lastRasterName = '3B-HHR-L.MS.MRG.3IMERG.20171130-S233000-E235959.1410.V05B.30min'
        lastFTPSubFolder = "/" + str(myConfig['ftp_subfolder'])+'/'+curr_month+'/'
        print lastFTPSubFolder
        minutesS=''
        minutesE=''
        mString=''
        scratchFolder = myConfig['scratchFolder']
        coor_system = 'WGS 1984'

        if int(curr_minutes)>30:
			minutesS='30'
			minutesE='59'
        else:
			minutesS='00'
			minutesE='29'
        mString=(2*int(curr_hour)*30)+(int(minutesE)-29)
        mStringS=str(mString)
        if len(str(int(curr_date)-1))<2:
			curr_date='0'+str(int(curr_date)-1)
        if len(mStringS)<4:
			leng=len(mStringS)
			diff=4-leng
			while diff>0:
				mStringS='0'+mStringS
				diff=diff-1
        #lastRasterName='3B-HHR-L.MS.MRG.3IMERG.'+curr_year+curr_month+curr_date+'-S'+curr_hour+minutesS+'00-E'+curr_hour+minutesE+'59.'+mStringS+'.V05B.30min'
        lastRasterName='3B-HHR-L.MS.MRG.3IMERG.'+curr_year+curr_month+curr_date+'-S233000-E235959.1410.V05B.30min'
        # KS Refactor 2015-06-26 # Refactor to place these accumulations in the same folder with the 3hr rasters..
        pathToGeoDB = str(myConfig['geoDB']) 
        print "hellooo befoew try"
        try:
            Download_And_Load_CustomRaster(lastRasterName, "1day", lastFTPSubFolder, ftpParams, scratchFolder, coor_system, pathToGeoDB, "IMERG1Day") # "TRMM1Day")
        except:
            e1 = sys.exc_info()[0]
            addToLog("PostETL_Support_Build_Custom_Rasters: ERROR, Something went wrong when attempting to download and load custom raster 1day to IMERG1Day.  System Error Message: "+ str(e1))
        try:
            Download_And_Load_CustomRaster(lastRasterName, "3day", lastFTPSubFolder, ftpParams, scratchFolder, coor_system, pathToGeoDB, "IMERG3Day") # "TRMM3Day")
        except:
            e3 = sys.exc_info()[0]
            addToLog("PostETL_Support_Build_Custom_Rasters: ERROR, Something went wrong when attempting to download and load custom raster 3day to IMERG3Day.  System Error Message: "+ str(e3))
        try:
            Download_And_Load_CustomRaster(lastRasterName, "7day", lastFTPSubFolder, ftpParams, scratchFolder, coor_system, pathToGeoDB, "IMERG7Day") # "TRMM7Day")
        except:
            e7 = sys.exc_info()[0]
            addToLog("PostETL_Support_Build_Custom_Rasters: ERROR, Something went wrong when attempting to download and load custom raster 7day to IMERG7Day.  System Error Message: "+ str(e7))
    except:
        e = sys.exc_info()[0]
        addToLog("PostETL_Support_Build_Custom_Rasters: ERROR, Something went wrong when attempting to download and load custom rasters.  System Error Message: "+ str(e))


# Stops the TRMM services, runs the custom raster generation routine, then restarts the TRMM services
def PostETL_Do_Update_Service_And_Custom_Rasters(PostETL_CustomRaster_Params, service_Options_List):

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
            addToLog("PostETL_Do_Update_Service_And_CustomRasters: ERROR, Stop Service failed for " + str(current_ServiceName) + ", System Error Message: "+ str(e))



    # Run the code for creating custom rasters
    addToLog("PostETL_Do_Update_Service_And_CustomRasters: About to update Custom Rasters")
    try:
        PostETL_Support_Build_Custom_Rasters(PostETL_CustomRaster_Params)
    except:
        e = sys.exc_info()[0]
        addToLog("PostETL_Do_Update_Service_And_CustomRasters: ERROR, Something went wrong while building TRMM Custom Rasters, System Error Message: "+ str(e))



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

def PostETL_Controller_Method():
    # Do a "PostETL" Process

    # Gathering inputs
    rasterOutputLocation = 'E:\SERVIR\Data\Global\IMERG'
    intervalString = '90 days'
    regExp_Pattern = '\d{4}[01]\d[0-3]\d[0-2]\d'
    rastDateFormat = '%Y%m%d%H'
    theVarList = PreETL_Support_Get_Standard_VarDictionary_From_RawVarSettings(settingsObj['VariableDictionaryList'])
    oldDate = Unsorted_GetOldestDate(intervalString) #= ETL_TransportObject['Extract_Object']['ResultsObject']['OldestDateTime'] # oldDateTime = datetime.datetime.strptime("2014-05-03T12", "%Y-%m-%dT%H")

    GeoDB_Workspace = 'E:\SERVIR\Data\Global\IMERG_SR3857.gdb'
    queryDateFormat = '%Y-%m-%d %H:00:00'
    # Inputs for the 3 Custom Raster generations.
    theOutputBasePath = GeoDB_Workspace #'C:\\kris\\!!Work\\ETL_TRMM\\GeoDB\\TRMM.gdb'

    PostETL_CustomRaster_Params = {
        'fileFolder_With_TRMM_Rasters' : rasterOutputLocation, # r'C:\ksArcPy\trmm\rastout',
        'color_map' : 'D:\SERVIR\Scripts\IMERG\SupportFiles\trmm_3hour.clr',  # r'C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_3hour.clr',
        'output_basepath' : theOutputBasePath,
        'raster_catalog_fullpath' : theOutputBasePath + '\\' + theVarList[0]['mosaic_name'],  # \\TRMM', # Should be a setting  mosaic_name
        'raster_catalog_options_datetime_field' : theVarList[0]['primary_date_field'],  # 'timestamp',
        'raster_catalog_options_datetime_sql_cast' : 'date',
        'raster_catalog_options_datetime_field_format' : queryDateFormat,  # '%Y-%m-%d %H:00:00',
        'start_datetime' : datetime.utcnow(),
        'trmm1Day_RasterCatalogName' : 'IMERG1Day',  #  'TRMM1Day',
        'trmm7Day_RasterCatalogName' : 'IMERG7Day',  #  'TRMM7Day',
        'trmm30Day_RasterCatalogName' : 'IMERG30Day',  #  'TRMM30Day',
        'trmm1Day_ColorMapLocation' : 'D:\SERVIR\Scripts\IMERG\SupportFiles\trmm_1day.clr',  #  r'C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_1day.clr',
        'trmm7Day_ColorMapLocation' : 'D:\SERVIR\Scripts\IMERG\SupportFiles\trmm_7day.clr',  #  r'C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_7day.clr',
        'trmm30Day_ColorMapLocation' : 'D:\SERVIR\Scripts\IMERG\SupportFiles\TRMM_30Day.clr',  #  r'C:\kris\!!Work\ETL_TRMM\SupportFiles\TRMM_30Day.clr',
        'workSpacePath' : 'Z:\ETLscratch\IMERG\PostETL' # r'C:\kris\!!Work\ETL_TRMM\ScratchWorkspace\custom_RenameLater'

    }
    pkl_file = open('config.pkl', 'rb')
    myConfig = pickle.load(pkl_file)
    pkl_file.close()
    # service_Options_List
    Service_Options_Accumulations = [
    {
        "Description":"IMERG Accumulations Service",
        "admin_dir_URL":myConfig['admin_dir_URL'],
        "username":myConfig['username'],
        "password":myConfig['password'],
        "folder_name":myConfig['folder_name'],
        "service_name":"IMERG_Accumulations",
        "service_type":myConfig['service_type']
    }]
    # Update Service and Build new static composits.
    try:

		PostETL_Do_Update_Service_And_Custom_Rasters(PostETL_CustomRaster_Params, Service_Options_Accumulations)
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
    folder_Pre = 'Z:\ETLscratch\IMERG\PreETL'
    folder_E = 'Z:\ETLscratch\IMERG\Extract'
    folder_T = 'Z:\ETLscratch\IMERG\Transform'
    folder_L = 'Z:\ETLscratch\IMERG\Load'
    folder_Post = 'Z:\ETLscratch\IMERG\PostETL'
    addToLog("PostETL_Controller_Method: Scratch Workspaces should now be cleaned")


    # Refresh the list of Permissions for accumulation rasters
    rasterDatasetList = ["IMERG1Day","IMERG3Day", "IMERG7Day", "IMERG30Day"]
    addToLog("End of etl2")
	
    PostETL_RefreshPermissions_For_Accumulations(GeoDB_Workspace, rasterDatasetList)
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

#--------------------------------------------------------------------------
# Controller
#--------------------------------------------------------------------------
def main(config_Settings):

    # Get a start time for the entire script run process.
    time_TotalScriptRun_Process = get_NewStart_Time()

    # Clear way to show entry in the log file for a script session start
    addToLog("======================= SESSION START ACCUMULATIONS =======================")

    # Config Settings
    settingsObj = config_Settings.xmldict['ConfigObjectCollection']['ConfigObject']

    # Access to the Config settings example
    addToLog("Script Session Name is: IMERG ACCUMULATIONS ETL")

    # Set up Detailed Logging
    current_DetailedLogging_Setting = settingsObj['DetailedLogging']
    global g_DetailedLogging_Setting
    if current_DetailedLogging_Setting == '1':
        g_DetailedLogging_Setting = True
    else:
        g_DetailedLogging_Setting = False
    addToLog("Main: Detailed logging has been enabled", True)

    # Execute Post ETL, Log the Time, and load the Results object.
    time_PostETL_Process = get_NewStart_Time()
    try:
        addToLog("========= Post ETL =========")
        PostETL_Controller_Method()
    except:
        e = sys.exc_info()[0]
        addToLog("main: Post ETL ERROR, something went wrong, ERROR MESSAGE: "+ str(e))
    addToLog("TIME PERFORMANCE: time_PostETL_Process : " + get_Elapsed_Time_As_String(time_PostETL_Process))

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