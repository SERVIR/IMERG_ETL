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


# Gets and returns a list of files contained in the bucket and path.
#   Access Keys are required and are used for making a connection object.
def Extract_Support_s3_GetFileListForPath(s3_AccessKey,s3_SecretKey,s3_BucketName, s3_PathToFiles, s3_Is_Use_Local_IAMRole):
    s3_Connection = None
    if s3_Is_Use_Local_IAMRole == True:
        try:
            s3_Connection = boto.connect_s3(is_secure=False)
        except:
            s3_Connection = boto.connect_s3(s3_AccessKey, s3_SecretKey,is_secure=False)
    else:
        s3_Connection = boto.connect_s3(s3_AccessKey, s3_SecretKey,is_secure=False)

    s3_Bucket = s3_Connection.get_bucket(s3_BucketName,True,None)
    s3_ItemsList = list(s3_Bucket.list(s3_PathToFiles))
    retList = []
    for current_s3_Item in s3_ItemsList:
        retList.append(current_s3_Item.key)
    return retList


# Takes in a key and converts it to a URL.
def Extract_Support_s3_Make_URL_From_Key(s3_BucketRootPath, current_s3_Key):
    # Sample URL    3 (yes, 2 slashes, does not work with only 1)
    # https://bucket.servirglobal.net.s3.amazonaws.com//regions/africa/data/eodata/crest/TIFQPF2014021812.zip
    retString = str(s3_BucketRootPath) + str(current_s3_Key)
    return retString

# Get the file name portion of an S3 Key Path
def Extract_Support_Get_FileNameOnly_From_S3_KeyPath(theS3KeyPath):
    retStr = theS3KeyPath.split('/')[-1]
    return retStr

# Try and Extract a 'gz' file, if success, return True, if fail, return False
# Example inputs
# inFilePath = r"d:\fullpath\TRMM-3B42RT-V7-Rain_2014-05-03T03Z.tif.gz"
# outFilePath = r"d:\fullpath\TRMM-3B42RT-V7-Rain_2014-05-03T03Z.tif",
# inFileExt = "tif.gz"
def Extract_Support_Decompress_GZip_File(inFilePath, outFilePath, inFileExt):
    # make sure the format is correct.
    if "GZ" in inFileExt.upper():
        try:
            inF = gzip.open(inFilePath, 'rb')
            outF = open(outFilePath, 'wb')
            outF.write( inF.read() )
            inF.close()
            outF.close()
            addToLog("Extract_Support_Decompress_GZip_File: Extracted file from, " + str(inFilePath) + " to " + str(outFilePath), True)
            return True
        except:
            e = sys.exc_info()[0]
            addToLog("Extract_Support_Decompress_GZip_File: ERROR extracting the gz File " + str(inFilePath) + " Error Message: " + str(e))
            return False
    else:
        # File extension is incorrect.
        addToLog("Extract_Support_Decompress_GZip_File: ERROR, File " + str(inFilePath) + " has an unexpected file extension.")
        return False







# Get the file names, filter the list, download the files, extract them, wrap them into a list, return a results object
# Goes into the S3, downloads files, extracts them, returns list of items
def Extract_Do_Extract_S3(the_FileExtension, s3BucketRootPath, s3AccessKey, s3SecretKey, s3BucketName, s3PathTo_Files, s3_Is_Use_Local_IAM_Role, regEx_String, dateFormat_String, startDateTime_str, endDateTime_str, theExtractWorkspace):

    ExtractList = []
    counter_FilesDownloaded = 0
    counter_FilesExtracted = 0
    debugFileDownloadLimiter = 10000      # For debugging, set this to a low number

    # Get the Start / End datetimes
    startDateTime = datetime.strptime(startDateTime_str, dateFormat_String)
    endDateTime = datetime.strptime(endDateTime_str, dateFormat_String)

     # get a list of ALL files from the bucket and path combo.
    theListOf_BucketPath_FileNames = Extract_Support_s3_GetFileListForPath(s3AccessKey,s3SecretKey,s3BucketName,s3PathTo_Files, s3_Is_Use_Local_IAM_Role)

    # get a list of all the files within the start and end date
    filePaths_WithinRange = Extract_Support_GetList_Within_DateRange(theListOf_BucketPath_FileNames, the_FileExtension, startDateTime, endDateTime, regEx_String, dateFormat_String)

    numFound = len(filePaths_WithinRange)
    if numFound == 0:
        if startDateTime_str == endDateTime_str:
            addToLog("Extract_Do_Extract_S3: ERROR: No files found for the date string "+startDateTime_str)
        else:
            addToLog("Extract_Do_Extract_S3: ERROR: No files found between "+startDateTime_str+" and "+endDateTime_str)
    else:

        # Iterate through each key file path and and perform the extraction.
        for s3_Key_file_Path_to_download in filePaths_WithinRange:
            if counter_FilesDownloaded < debugFileDownloadLimiter:
                file_to_download = Extract_Support_Get_FileNameOnly_From_S3_KeyPath(s3_Key_file_Path_to_download)

                # This is the location where the file will be downloaded.
                downloadedFile = os.path.join(theExtractWorkspace,file_to_download)   # Actual Code

                # Get final download path (URL to where the file is located on the internets.
                currentURL_ToDownload = Extract_Support_s3_Make_URL_From_Key(s3BucketRootPath, s3_Key_file_Path_to_download)

                # Do the actual download.
                try:
                    theDLodaedURL = urllib.urlopen(currentURL_ToDownload)
                    open(downloadedFile,"wb").write(theDLodaedURL.read())
                    theDLodaedURL.close()
                    addToLog("Extract_Do_Extract_S3: Downloaded file from: " + str(currentURL_ToDownload), True)
                    addToLog("Extract_Do_Extract_S3: Downloaded file to: " + str(downloadedFile), True)
                    counter_FilesDownloaded += 1

                    # Extract the zipped file, (NOTE, THIS IS FOR GZIP FILES, files with extension of .gz)
                    # Also, the expected end of the file name is, ".tif.gz"
                    # Last Note, this function only unzips a single file
                    theOutFile = downloadedFile[:-3] # Should be the whole file path except the '.gz' part.
                    ungzipResult = Extract_Support_Decompress_GZip_File(downloadedFile, theOutFile, the_FileExtension)
                    if ungzipResult == True:
                        # Extraction worked, create the return item
                        extractedFileList = []
                        extractedFileList.append(theOutFile)
                        currentDateString = Extract_Support_Get_DateString_From_String(theOutFile, regEx_String)
                        current_Extracted_Obj = {
                            'DateString' : currentDateString,
                            'Downloaded_FilePath' : downloadedFile,
                            'ExtractedFilesList' : convert_Obj_To_List(extractedFileList),
                            'downloadURL' : currentURL_ToDownload
                        }
                        ExtractList.append(current_Extracted_Obj)
                        counter_FilesExtracted += 1
                    else:
                        # Extraction failed, Add this to the log..
                        addToLog("Extract_Do_Extract_S3: ERROR, There was a problem decompressing the file, " + str(downloadedFile))

                except:
                    e = sys.exc_info()[0]
                    addToLog("Extract_Do_Extract_S3: ERROR: Could not download file: " + str(theDLodaedURL) + ", Error Message: " + str(e))
    ret_ExtractObj = {
        'StartDateTime':startDateTime,
        'EndDateTime': endDateTime,
        'ExtractList':ExtractList
    }
    return ret_ExtractObj

# Need to convert the IMERG S String back into an hour
def get_DateAdjusted_IMERG_RasterName_From_ActualRasterName(rasterName):
    # The possible hours
    h_00 = "-S000000-E002959.0000"
    h_03 = "-S030000-E032959.0180"
    h_06 = "-S060000-E062959.0360"
    h_09 = "-S090000-E092959.0540"
    h_12 = "-S120000-E122959.0720"
    h_15 = "-S150000-E152959.0900"
    h_18 = "-S180000-E182959.1080"
    h_21 = "-S210000-E212959.1260"
    if h_00 in rasterName:
        retRastName = rasterName.replace(h_00,"00")
        return retRastName
    if h_03 in rasterName:
        retRastName = rasterName.replace(h_03,"03")
        return retRastName
    if h_06 in rasterName:
        retRastName = rasterName.replace(h_06,"06")
        return retRastName
    if h_09 in rasterName:
        retRastName = rasterName.replace(h_09,"09")
        return retRastName
    if h_12 in rasterName:
        retRastName = rasterName.replace(h_12,"12")
        return retRastName
    if h_15 in rasterName:
        retRastName = rasterName.replace(h_15,"15")
        return retRastName
    if h_18 in rasterName:
        retRastName = rasterName.replace(h_18,"18")
        return retRastName
    if h_21 in rasterName:
        retRastName = rasterName.replace(h_21,"21")
        return retRastName
    # No Change
    return rasterName


# Update, Params returned in list,
#  Each object has these,
#   FTPFolderPath
#   BaseRasterName
#   FTP_PathTo_TIF
#   FTP_PathTo_TFW
# root_FTP_Path = "ftp://trmmopen.gsfc.nasa.gov/pub/gis"
# Returns objects which are, { partialRasterName: "3B42RT.2014052203.7.", basePath: "ftp://someftppath",  baseRasterName: "3B42RT.2014052203.7.03hr" , pathTo_TIF: "ftp://someftppath/3B42RT.2014052203.7.03hr.tif", pathTo_TFW: "someftppath/3B42RT.2014052203.7.03hr.tfw"}
# No 'basePath', instead, using 'FTPFolderPath'  (Path to the folder containing the current expected tif file.
# Partial Raster Name part can be used to construct a download for the 1day, or 3day or 7day files by appending, ".1day.tif" and ".1day.tfw" for example.  (Ofcourse the base path needs to be prepended to this to construct a full download link.)
# Each object represents a single raster with 2 files to download.
def Extract_Support_Get_Expected_FTP_Paths_From_DateRange(start_DateTime, end_DateTime, root_FTP_Path,
                                                          the_FTP_SubFolderPath):
    retList = []

    # counter = 0
    # Refactoring for new IMERG Datasource (04/2015)
    # Old TRMM Filename example     3B42RT.2014062612.7.03hr.tfw
    # new IMERG Filename example    3B-HHR-L.MS.MRG.3IMERG.20150401-S010000-E012959.0060.V03E.3hr.tfw
    # the old TRMM way

    # New IMERG Code
    the_DateFormatString = "%Y%m%d%H"  # When appending the string below, the hour component needs to be chopped off
    the_DateFormatString_ForFileName = "%Y%m%d"
    the_FileNamePart1 = "3B-HHR-L.MS.MRG.3IMERG."  # IMERG Product ID?
    the_FileNameEnd_3Hr_Base = ".V03E.3hr"  # Version and time frame
    the_FileNameEnd_Tif_Ext = ".tif"  # Tif file
    the_FileNameEnd_Tfw_Ext = ".tfw"  # World File

    # Unused, for reference     # a tif and tfw also exist for each of these..
    the_FileNameEnd_30min_Base = ".V03E.30min"  # Version and time composit product
    the_FileNameEnd_1day_Base = ".V03E.1day"  # Version and time composit product
    the_FileNameEnd_3day_Base = ".V03E.3day"  # Version and time composit product
    the_FileNameEnd_7day_Base = ".V03E.7day"  # Version and time composit product

    currentDateTime = start_DateTime
    while currentDateTime < end_DateTime:
        # Do processing
        # Build all the object props based on currentDateTime and filenames etc.. BUILD the folder paths
        currentDateString = currentDateTime.strftime(the_DateFormatString)
        currentYearString = currentDateTime.strftime("%Y")
        currentMonthString = currentDateTime.strftime("%m")
        currentDateString_ForFileName = currentDateTime.strftime(the_DateFormatString_ForFileName)

        # Needed for IMERG Refactor
        currentHourString = currentDateTime.strftime("%H")
        current_imerg_S_String = get_IMERG_S_HourString_From_Hour(int(currentHourString))
        currentRasterBaseName = the_FileNamePart1 + currentDateString_ForFileName + current_imerg_S_String + the_FileNameEnd_3Hr_Base
        currentFTP_Subfolder = the_FTP_SubFolderPath + "/" + currentMonthString  # IMERG TIF FTP has a different folder structure.. only the months..
        currentFTPFolder = root_FTP_Path + "/" + currentMonthString  # IMERG TIF FTP has a different folder structure.. only the months..
        current_3Hr_Tif_Filename = currentRasterBaseName + the_FileNameEnd_Tif_Ext
        current_3Hr_Twf_Filename = currentRasterBaseName + the_FileNameEnd_Tfw_Ext

        currentPathToTif = currentFTPFolder + "/" + current_3Hr_Tif_Filename
        currentPathToTwf = currentFTPFolder + "/" + current_3Hr_Twf_Filename

        # Load object
        # Create an object loaded with all the params listed above
        currentObj = {
            "FTPFolderPath": currentFTPFolder,
            "FTPSubFolderPath": currentFTP_Subfolder,
            "BaseRasterName": currentRasterBaseName,
            "FTP_PathTo_TIF": currentPathToTif,
            "FTP_PathTo_TFW": currentPathToTwf,
            "TIF_3Hr_FileName": current_3Hr_Tif_Filename,
            "TWF_3Hr_FileName": current_3Hr_Twf_Filename,
            "DateString": currentDateString
        }

        # Add object to list
        # Add the object to the return list.
        retList.append(currentObj)

        # Incremenet to next currentDateTime
        currentDateTime = currentDateTime + datetime.timedelta(hours=3)
    return retList

# To get the accumulations, Just use,
# current_RastObj = rastObjList[n]
# current_tif_Location = current_RastObj['FTP_PathTo_TIF']
# current_tfw_Location = current_RastObj['FTP_PathTo_TFW']
# current_1Day_tif_Location = current_tif_Location.replace(".7.03hr",".7.1day")
# current_1Day_tfw_Location = current_tfw_Location.replace(".7.03hr",".7.1day")
# current_3Day_tif_Location = current_tif_Location.replace(".7.03hr",".7.3day")
# current_3Day_tfw_Location = current_tfw_Location.replace(".7.03hr",".7.3day")
# current_7Day_tif_Location = current_tif_Location.replace(".7.03hr",".7.7day")
# current_7Day_tfw_Location = current_tfw_Location.replace(".7.03hr",".7.7day")
def debug_Get_CompositLocations_From_Raster(currentRasterObj):
    current_tif_Location = currentRasterObj['FTP_PathTo_TIF']
    current_tfw_Location = currentRasterObj['FTP_PathTo_TFW']
    current_1Day_tif_Location = current_tif_Location.replace(".7.03hr",".7.1day")
    current_1Day_tfw_Location = current_tfw_Location.replace(".7.03hr",".7.1day")
    current_3Day_tif_Location = current_tif_Location.replace(".7.03hr",".7.3day")
    current_3Day_tfw_Location = current_tfw_Location.replace(".7.03hr",".7.3day")
    current_7Day_tif_Location = current_tif_Location.replace(".7.03hr",".7.7day")
    current_7Day_tfw_Location = current_tfw_Location.replace(".7.03hr",".7.7day")
    retObj = {
        "current_tif_Location":current_tif_Location,
        "current_tfw_Location":current_tfw_Location,
        "current_1Day_tif_Location":current_1Day_tif_Location,
        "current_1Day_tfw_Location":current_1Day_tfw_Location,
        "current_3Day_tif_Location":current_3Day_tif_Location,
        "current_3Day_tfw_Location":current_3Day_tfw_Location,
        "current_7Day_tif_Location":current_7Day_tif_Location,
        "current_7Day_tfw_Location":current_7Day_tfw_Location
    }
    return retObj


# Returns a date time which has a new hour value (Meant for standardizing the hours to 3 hour increments)
def Extract_Support_Set_DateToStandard_3_Hour(hourValue, theDateTime):
    formatString = "%Y%m%d%H"
    newDateTimeString = theDateTime.strftime("%Y%m%d")
    if hourValue < 10:
        newDateTimeString += "0"
    newDateTimeString += str(hourValue)

    newDateTime = datetime.strptime(newDateTimeString, formatString)

    return newDateTime

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


def Extract_Do_Extract_FTP(dateFormat_String, startDateTime_str, endDateTime_str, theExtractWorkspace):
    addToLog("Extract_FTP: Started")  # , True)
    pkl_file = open('config.pkl', 'rb')
    myConfig = pickle.load(pkl_file)
    pkl_file.close()
    # Move these to settings at the earliest opportunity!!
    # IMERG Refactor, new ftp path is ftp://jsimpson.pps.eosdis.nasa.gov/data/imerg/gis/04/
    the_FTP_Host = myConfig[
        'ftp_host']  # "trmmopen.gsfc.nasa.gov" #"198.118.195.58" #trmmopen.gsfc.nasa.gov"  #"ftp://trmmopen.gsfc.nasa.gov"
    the_FTP_SubFolderPath = myConfig['ftp_subfolder']  # "pub/gis"
    the_FTP_UserName = myConfig['ftp_user']  # "anonymous" #
    the_FTP_UserPass = myConfig['ftp_pswrd']  # "anonymous" #"anonymous" #
    root_FTP_Path = "ftp://" + str(the_FTP_Host) + "/" + the_FTP_SubFolderPath
    ExtractList = []
    lastBaseRaster = ""
    lastFTPFolder = ""
    counter_FilesDownloaded = 0
    counter_FilesExtracted = 0
    debugFileDownloadLimiter = 5000

    # Get the Start / End datetimes

    startDateTime = startDateTime_str
    endDateTime = datetime.strptime(endDateTime_str, dateFormat_String)

    addToLog("Extract_FTP: dateFormat_String: " + str(dateFormat_String))
    addToLog("Extract_FTP: startDateTime: " + str(startDateTime))
    addToLog("Extract_FTP: endDateTime: " + str(endDateTime))

    # KS Refactor For 30 Min Datasets (These next two lines work just fine for the 3 hour dataset... replacing them with a function that adjusts for the next 30 min increment)
    # Start Date adjustment
    newStart_Minute = Extract_Support_Get_Last_30_Min(startDateTime.minute)
    standardized_StartDate = Extract_Support_Set_DateToStandard_30_Minute(newStart_Minute, startDateTime)
    # KS Refactor For 30 Min Datasets (These next two lines work just fine for the 3 hour dataset... replacing them with a function that adjusts for the next 30 min increment)
    # End Date adjustment
    newEnd_Minute = Extract_Support_Get_Last_30_Min(endDateTime.minute)
    standardized_EndDate = Extract_Support_Set_DateToStandard_30_Minute(newEnd_Minute, endDateTime)
    # KS Refactor For 30 Min Datasets // Created a couple of new functions including 'Extract_Support_Get_Expected_FTP_Paths_From_DateRange_For_30Min_Datasets' to handle 30 min files
    # Extract_Support_Get_Expected_FTP_Paths_From_DateRange_For_30Min_Datasets
    # get a list of all the files within the start and end date
    expected_FilePath_Objects_To_Extract_WithinRange = Extract_Support_Get_Expected_FTP_Paths_From_DateRange_For_30Min_Datasets(
        standardized_StartDate, standardized_EndDate, root_FTP_Path, the_FTP_SubFolderPath)
    addToLog("Extract_FTP: expected_FilePath_Objects_To_Extract_WithinRange (list to process) " + str(
        expected_FilePath_Objects_To_Extract_WithinRange), True)

    # KS Refactor for Early Data // Storying the Error Rasters in the return object
    errorRasters_List = []

    numFound = len(expected_FilePath_Objects_To_Extract_WithinRange)

    if numFound == 0:

        if startDateTime_str == endDateTime_str:
            addToLog("Extract_FTP: ERROR: No files found for the date string " + startDateTime_str)
        else:
            addToLog("Extract_FTP: ERROR: No files found between " + startDateTime_str + " and " + endDateTime_str)
    else:

        # Connect to FTP Server
        try:

            # QUICK REFACTOR NOTE: Something very strange was happening with the FTP and there isn't time to debug this issue.. going with URL Download instead for now.
            addToLog("Extract_FTP: Connecting to FTP", True)
            ftp_Connection = ftplib.FTP(the_FTP_Host, the_FTP_UserName, the_FTP_UserPass)
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
                        Tif_file_to_download = curr_FilePath_Object[
                            'TIF_30Min_FileName']  # ['TIF_3Hr_FileName']     # KS Refactor For 30 Min Datasets // Previous Rename affected this line
                        try:
                            downloadedFile_TIF = os.path.join(theExtractWorkspace, Tif_file_to_download)
                            with open(downloadedFile_TIF, "wb") as f:

                                ftp_Connection.retrbinary("RETR %s" % Tif_file_to_download, f.write)
                                time.sleep(1)
                        except:
                            os.remove(os.path.join(theExtractWorkspace, Tif_file_to_download))
                            Tif_file_to_download = Tif_file_to_download.replace("03E", "04A")
                            downloadedFile_TIF = os.path.join(theExtractWorkspace, Tif_file_to_download)
                            try:
                                with open(downloadedFile_TIF, "wb") as f:
                                    ftp_Connection.retrbinary("RETR %s" % Tif_file_to_download, f.write)
                                    time.sleep(1)
                            except:
                                os.remove(os.path.join(theExtractWorkspace, Tif_file_to_download))
                                Tif_file_to_download = Tif_file_to_download.replace("04A", "04B")
                                downloadedFile_TIF = os.path.join(theExtractWorkspace, Tif_file_to_download)
                                try:
                                    with open(downloadedFile_TIF, "wb") as f:
                                        ftp_Connection.retrbinary("RETR %s" % Tif_file_to_download, f.write)
                                        time.sleep(1)
                                except:
                                    addToLog("", True)

                        Twf_file_to_download = curr_FilePath_Object[
                            'TWF_30Min_FileName']  # ['TWF_3Hr_FileName']    # KS Refactor For 30 Min Datasets // Previous Rename affected this line
                        try:
                            downloadedFile_TFW = os.path.join(theExtractWorkspace, Twf_file_to_download)
                            with open(downloadedFile_TFW, "wb") as f:
                                ftp_Connection.retrbinary("RETR %s" % Twf_file_to_download, f.write)
                                time.sleep(1)
                        except:
                            os.remove(os.path.join(theExtractWorkspace, Twf_file_to_download))
                            Twf_file_to_download = Twf_file_to_download.replace("03E", "04A")
                            downloadedFile_TFW = os.path.join(theExtractWorkspace, Twf_file_to_download)
                            try:
                                with open(downloadedFile_TFW, "wb") as f:
                                    ftp_Connection.retrbinary("RETR %s" % Twf_file_to_download, f.write)
                                    time.sleep(1)
                            except:
                                os.remove(os.path.join(theExtractWorkspace, Twf_file_to_download))
                                Twf_file_to_download = Twf_file_to_download.replace("04A", "04B")
                                downloadedFile_TFW = os.path.join(theExtractWorkspace, Twf_file_to_download)
                                try:
                                    with open(downloadedFile_TFW, "wb") as f:
                                        ftp_Connection.retrbinary("RETR %s" % Twf_file_to_download, f.write)
                                        time.sleep(1)
                                except:
                                    addToLog("", True)

                                    # Two files were downloaed (or 'extracted') but we really only need a reference to 1 file (thats what the transform expects).. and Arc actually understands the association between the TIF and TWF files automatically
                        extractedFileList = []
                        extractedFileList.append(downloadedFile_TIF)
                        current_Extracted_Obj = {
                            'DateString': curr_FilePath_Object['DateString'],
                            'DateString_WithMinutes': curr_FilePath_Object['DateString_WithMinutes'],
                        # KS Refactor For 30 Min Datasets // Added more detailed DateString
                            'Downloaded_FilePath': downloadedFile_TIF,
                            'ExtractedFilesList': convert_Obj_To_List(extractedFileList),
                            'downloadURL': curr_FilePath_Object['FTP_PathTo_TIF'],  # currentURL_ToDownload
                            'FTP_DataObj': curr_FilePath_Object
                        }
                        ExtractList.append(current_Extracted_Obj)
                        lastBaseRaster = curr_FilePath_Object['BaseRasterName']
                        lastFTPFolder = curr_FilePath_Object['FTPSubFolderPath']
                        counter_FilesDownloaded += 1
                        if counter_FilesDownloaded % 100 == 0:  # if counter_FilesDownloaded % 20 == 0:
                            addToLog("Extract_FTP: Downloaded " + str(counter_FilesDownloaded) + " Rasters ....")

                    except:
                        # If the raster file is missing or an error occurs during transfer..
                        addToLog("Extract_FTP: ERROR.  Error downloading current raster " + str(
                            curr_FilePath_Object['BaseRasterName']))
                        addToLog(Twf_file_to_download)
                        # KS Refactor for Early Data // Storying the Error Rasters in the return object
                        errorRasters_List.append(str(curr_FilePath_Object['BaseRasterName']))


        except:
            e = sys.exc_info()[0]
            errMsg = "Extract_FTP: ERROR: Could not connect to FTP Server, Error Message: " + str(e)

    addToLog("Extract_FTP: Total number of rasters downloaded: " + str(counter_FilesDownloaded))

    ret_ExtractObj = {
        'StartDateTime': startDateTime,
        'EndDateTime': endDateTime,
        'ExtractList': ExtractList,
        'lastBaseRaster': lastBaseRaster,
        'lastFTPFolder': lastFTPFolder,
        'errorRasters_List': errorRasters_List
    # KS Refactor for Early Data // Storying the Error Rasters in the return object
    }

    return ret_ExtractObj

# FTP Extract

def get_IMERG_S_HourString_From_Hour(theHour):
    if theHour == 0:
        return "-S000000-E002959.0000"
    elif theHour == 3:
        return "-S030000-E032959.0180"
    elif theHour == 6:
        return "-S060000-E062959.0360"
    elif theHour == 9:
        return "-S090000-E092959.0540"
    elif theHour == 12:
        return "-S120000-E122959.0720"
    elif theHour == 15:
        return "-S150000-E152959.0900"
    elif theHour == 18:
        return "-S180000-E182959.1080"
    elif theHour == 21:
        return "-S210000-E212959.1260"
    else:
        addToLog("get_IMERG_S_HourString_From_Hour: ERROR. Value passed in for param (theHour) was: " + str(theHour) + " Which is not one of these expected values of, '00, 03, 06, 09, 12, 15, 18, 21' ")
        return "__ERROR IN function: get_IMERG_S_HourString_From_Hour, value passed in for param (theHour) was: " + str(theHour) + " ERROR__"




# KS Refactor For 30 Min Datasets
# Returns a date time which has a new minute value (Meant for standardizing the minutes to 30 minute increments)
def Extract_Support_Set_DateToStandard_30_Minute(minuteValue, theDateTime):
    formatString = "%Y%m%d%H%M"
    newDateTimeString = theDateTime.strftime("%Y%m%d%H")
    if minuteValue < 10:
        newDateTimeString += "0"
    newDateTimeString += str(minuteValue)

    newDateTime = datetime.strptime(newDateTimeString, formatString)

    return newDateTime


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
        currentFTP_Subfolder = the_FTP_SubFolderPath + "/" + currentMonthString # IMERG TIF FTP has a different folder structure.. only the months..
        currentFTPFolder = root_FTP_Path + "/" + currentMonthString # IMERG TIF FTP has a different folder structure.. only the months..
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
        currentDateTime = currentDateTime + timedelta(minutes=30)

    return retList




# KS Refactor For 30 Min Datasets
# Get last 30 min value from current
def Extract_Support_Get_Last_30_Min(currentMin):
    minToReturn = None
    if currentMin < 30:
        minToReturn = 0
    else:
        minToReturn = 30
    return minToReturn


def Extract_Controller_Method(ETL_TransportObject):
    # Check the setup for errors as we go.
    IsError = False
    ErrorMessage = ""

    # Get inputs for the next function

    # Inputs from ETL_TransportObject['SettingsObj']
    try:
        the_FileExtension = ETL_TransportObject['SettingsObj'][
            'Download_File_Extension']  # TRMM_FileExtension # TRMM_File_Extension
        s3BucketRootPath = ETL_TransportObject['SettingsObj']['s3_BucketRootPath']
        s3AccessKey = ETL_TransportObject['SettingsObj']['s3_AccessKeyID']
        s3SecretKey = ETL_TransportObject['SettingsObj']['s3_SecretAccessKey']
        s3BucketName = ETL_TransportObject['SettingsObj']['s3_BucketName']
        s3PathTo_Files = ETL_TransportObject['SettingsObj']['s3_PathTo_TRMM_Files']
        s3_Is_Use_Local_IAM_Role = get_BoolSetting(ETL_TransportObject['SettingsObj']['s3_UseLocal_IAM_Role'])
        regEx_String = ETL_TransportObject['SettingsObj']['RegEx_DateFilterString']
        dateFormat_String = ETL_TransportObject['SettingsObj']['Python_DateFormat']
        extractWorkspace = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations'][
            'Extract']
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
        mosaicName = varList[0][
            'mosaic_name']  # ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Variable_Dictionary_List']
        primaryDateField = varList[0]['primary_date_field']
        mosaicDS = os.path.join(GeoDB_Workspace, mosaicName)

        startDateTime = Extract_Support_GetStartDate(primaryDateField, mosaicDS)

        # KS Refactor For 30 Min Datasets  (original string for the 3 hour dataset "%Y%m%d%H")
        dateFormat_String = "%Y%m%d%H%M"
        try:
            endDateTime = datetime.datetime.utcnow()
        # x=time.strptime("2017-04-21 01:00:00","%Y-%m-%d %H:%M:00")
        # xd=datetime.fromtimestamp(mktime(x))
        # endDateTime = xd
        except:
            et = datetime.utcnow().strftime("%Y-%m-%d %H:%M:00")
            endDateTime = datetime.strptime(et, "%Y-%m-%d %H:%M:%S")
        startDateTime_str = datetime.strptime(startDateTime, "%Y-%m-%d %H:%M:%S")

        endDateTime_str = endDateTime.strftime(dateFormat_String)
    except:
        e = sys.exc_info()[0]
        errMsg = "Extract_Controller_Method: ERROR: Could not get Dates, Error Message: " + str(e)
        addToLog(errMsg)
        IsError = True
        ErrorMessage += "|  " + errMsg
    addToLog(
        "Extract_Controller_Method: Using startDateTime_str : startDateTime : " + str(startDateTime_str) + " : " + str(
            startDateTime))
    addToLog("Extract_Controller_Method: Using endDateTime_str : endDateTime :  " + str(endDateTime_str) + " : " + str(
        endDateTime))

    # Execute the Extract Process.
    ExtractResult = Extract_Do_Extract_FTP(dateFormat_String, startDateTime_str, endDateTime_str, extractWorkspace)

    if len(ExtractResult['ExtractList']) == 0:
        IsError = True
        ErrorMessage += "|  Extract List contains 0 elements.  No files were extracted."

    # Package up items from the PreETL Step
    returnObj = {
        'ExtractResult': ExtractResult,
        'OldestDateTime': startDateTime,
        'IsError': IsError,
        'ErrorMessage': ErrorMessage
    }

    # Return the packaged items.
    return returnObj

#--------------------------------------------------------------------------
# Transform
#--------------------------------------------------------------------------

# See "Pre ETL" Section for the format of these functions
def Transform_ExampleSupportMethod():
    pass

