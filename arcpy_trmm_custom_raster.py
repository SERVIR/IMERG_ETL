# Developer: Daniel Duarte
# Company:   Spatial Development International
# E-mail:    dduarte@spatialdev.com


# standard library
import os
import sys
from datetime import datetime, timedelta
import shutil

# third-party
from arcpy.sa import *
import arcpy



class TRMMCustomRasterRequest:

    """ encapsulates a request to the TRMMCustomRasterCreator to create a custom raster from the TRMM raster catalog.

        request_options <dict>: contains the following additional options:

            factory_specifications <dict>: options for the output raster

                'output_raster_fullpath' <str>: the fullpath and name of the output raster
                'clip_extent' <str>: the processing extent contained within "-180.0 -50.0 180.0 50.0" and given in the same format "xMin yMin xMax yMax"
                'clip_raster' <arcpy.Raster>: the fullpath to a raster to be used in clipping the TRMM output raster using arcpy.Clip_management
                'CopyRaster_management_config' <dict>: config for arcpy.CopyRaster_Management
                'AddColormap_management_config' <dict>: config for arcpy.AddColormap_management

            input_raster_catalog_options <dict>: options for the input raster catalog

                'raster_catalog_fullpath' <str>: fullpath to the source raster catalog
                "raster_name_field" <str>: the field in the raster catalog that contains the names of the rasters
                "datetime_field" <str>: the field in the raster catalog that contains the datetime for the rasters
                'datetime_sql_cast' <str>: the DATETIME cast expression based on the underlying SQL type ex: "date"
                'datetime_field_format' <str>: the format of the given datetime_field. ex: '%m-%d-%Y %I:%M:%S %p',
                'start_datetime' <str>: the start datetime given in the format %Y%m%d%H with 'h' being a 24-hour. ex: "2012-02-29 at 3PM == 201202291500"
                'end_datetime' <str>: the end datetime given in the format %Y%m%d%H with 'h' being a 24-hour. ex: "2012-02-29 at 3PM == 201202291500"
    """

    def __init__(self, request_options):

        # Fix for no logger # Fix for logger (One of these classes is not hooked to the global debug logger.....)
        self.debug_logger = request_options['debug_logger'] #raster_creator_options.get('debug_logger',lambda*a,**kwa:None)
        #self.debug_logger = request_options.get('debug_logger',lambda*a,**kwa:None) #raster_creator_options.get('debug_logger',lambda*a,**kwa:None)

        self.factory_specifications = request_options['factory_specifications']
        self.input_raster_catalog_options = request_options['input_raster_catalog_options']

    def getFactorySpecifications(self):

        return self.factory_specifications

    def getRasterCatalogFullpath(self):

        return self.input_raster_catalog_options['raster_catalog_fullpath']

    #def extractRastersToWorkspace(self, path_to_extract_into):
    def extractRastersToWorkspace(self, path_to_extract_into, fileFolder_With_TRMM_Rasters):

        #self.debug_logger("ALERT 1")

        rasters_to_extract_list = self._getListOfRasterNamesFromRasterCatalog()

        #self.debug_logger("ALERT 2a rasters_to_extract_list " + str(rasters_to_extract_list))
        #self.debug_logger("ALERT 2b path_to_extract_into " + str(path_to_extract_into))
        #self.debug_logger("ALERT 2c fileFolder_With_TRMM_Rasters " + str(fileFolder_With_TRMM_Rasters))
        #extracted_rasters_list = self._extractRastersFromRasterCatalog(rasters_to_extract_list, path_to_extract_into)

        extracted_rasters_list = self._extractRastersFromRasterCatalog(rasters_to_extract_list, path_to_extract_into, fileFolder_With_TRMM_Rasters)
        #self.debug_logger("ALERT 3 extracted_rasters_list " + str(extracted_rasters_list))

        return extracted_rasters_list

    def _getListOfRasterNamesFromRasterCatalog(self, additional_where_clause=""):

        raster_name_field = self.input_raster_catalog_options['raster_name_field']
        where_clause = self._createWhereClause()
        where_clause += self.input_raster_catalog_options.get('additional_where_clause',"")

        #self.debug_logger("ALERT 1.1a Params self.input_raster_catalog_options['raster_catalog_fullpath'], " + str(self.input_raster_catalog_options['raster_catalog_fullpath']))
        #self.debug_logger("ALERT 1.1b Params where_clause, " + str(where_clause))
        #self.debug_logger("ALERT 1.1c Params raster_name_field, " + str(raster_name_field))
        rows = arcpy.SearchCursor(self.input_raster_catalog_options['raster_catalog_fullpath'], where_clause, "", raster_name_field)
        try:
            rasterNames = [str(row.getValue(raster_name_field)) for row in rows]
            #self.debug_logger("ALERT 1.2 rasterNames " + str(rasterNames))
            return rasterNames
        except Exception as e:
            pass
            #self.debug_logger("ALERT 1.3 ERROR in _getListOfRasterNamesFromRasterCatalog Message " + str(e))
        finally:
            del rows

    def _createWhereClause(self):

        start_datetime = self.input_raster_catalog_options['start_datetime']
        end_datetime = self.input_raster_catalog_options['end_datetime']
        datetime_field = self.input_raster_catalog_options['datetime_field']
        datetime_field_format = self.input_raster_catalog_options['datetime_field_format']
        datetime_sql_cast = self.input_raster_catalog_options['datetime_sql_cast']

        where_clause = "%s <= %s \'%s\'" % (datetime_field, datetime_sql_cast, start_datetime.strftime(datetime_field_format))
        where_clause += " AND %s >= %s \'%s\'" % (datetime_field, datetime_sql_cast, end_datetime.strftime(datetime_field_format))

        return where_clause

    # Expects something simillar to ['C:', 'ksArcPy\trmm\rastout', 'TRMM-3B42RT-V7-Rain_2014-06-17T18Z.tif']
    def _ksReplacementSupport_Get_RootPath_FromParts(self, theParts):
        #self.debug_logger("ALERT 3.1: theParts val : " + str(theParts))
        rootRetPath = ""
        inPartsCounter = 0
        inPartsMax = len(theParts) - 1
        for currPart in theParts:
            if inPartsCounter < inPartsMax:
                rootRetPath += currPart
                rootRetPath += "\\"
            inPartsCounter += 1
        #self.debug_logger("ALERT 3.2: rootRetPath final val : " + str(rootRetPath))
        return rootRetPath

    # Replacement for ArcpyCopyRaster Management which seems to truncate our rasters at this step..
    def _ksReplacement_CopyRaster_management(self, currentRaster_FileString, currentOutputCopy_FullPathAndFile):
        # Get input parts
        inParts = currentRaster_FileString.split("\\")          # expected, "c:\path\to\InRaster\rastoutFileName.tif"       (Split to a list)
        inTifFile = inParts[-1]                                 # expected, "rastoutFileName.tif"
        inFileBaseName = inTifFile.split(".")[0]                # expected, "rastoutFileName"
        rootInPath = self._ksReplacementSupport_Get_RootPath_FromParts(inParts) # expected, "c:\path\to\InRaster\"
        sourceFilePath_1 = rootInPath + inFileBaseName + ".tfw"
        sourceFilePath_2 = rootInPath + inFileBaseName + ".tif"
        sourceFilePath_3 = rootInPath + inFileBaseName + ".tif.aux.xml"
        sourceFilePath_4 = rootInPath + inFileBaseName + ".tif.ovr"
        sourceFilePath_5 = rootInPath + inFileBaseName + ".tif.xml"

        # Get output parts
        outParts = currentOutputCopy_FullPathAndFile.split("\\")    # expected, "c:\path\to\OutRaster\ScratchOutputRasterFileName.tif"        (Split to a list)
        outTifFile = outParts[-1]                                   # expected, "ScratchOutputRasterFileName.tif"
        outFileBaseName = outTifFile.split(".")[0]                  # expected, "ScratchOutputRasterFileName"
        rootOutPath = self._ksReplacementSupport_Get_RootPath_FromParts(outParts) # expected, "c:\path\to\OutRaster\"
        destFilePath_1 = rootOutPath + outFileBaseName + ".tfw"
        destFilePath_2 = rootOutPath + outFileBaseName + ".tif"
        destFilePath_3 = rootOutPath + outFileBaseName + ".tif.aux.xml"
        destFilePath_4 = rootOutPath + outFileBaseName + ".tif.ovr"
        destFilePath_5 = rootOutPath + outFileBaseName + ".tif.xml"

        # perform the multiple filesystem copies
        shutil.copyfile(sourceFilePath_1,destFilePath_1)
        shutil.copyfile(sourceFilePath_2,destFilePath_2)
        shutil.copyfile(sourceFilePath_3,destFilePath_3)
        shutil.copyfile(sourceFilePath_4,destFilePath_4)
        shutil.copyfile(sourceFilePath_5,destFilePath_5)




    #def _extractRastersFromRasterCatalog(self, rasters_to_extract_list, path_to_extract_into):
    def _extractRastersFromRasterCatalog(self, rasters_to_extract_list, path_to_extract_into, fileFolder_With_TRMM_Rasters):

        extracted_raster_list = []

        joinPath = os.path.join
        #self.debug_logger("ALERT 2.1")

        raster_name_field = self.input_raster_catalog_options['raster_name_field']
        #self.debug_logger("ALERT 2.2 raster_name_field " + str(raster_name_field))

        output_raster_catalog = self.input_raster_catalog_options['raster_catalog_fullpath']
        #self.debug_logger("ALERT 2.3 output_raster_catalog " + str(output_raster_catalog))


        # Whoops, more new logic
        # Rasters are already extracted and stored at 'fileFolder_With_TRMM_Rasters'
        # What we need to do is, copy them into 'path_to_extract_into' by building the existing and final file paths..
        for the_Raster_Name in rasters_to_extract_list:
            #self.debug_logger("ALERT 2.3a_pre the_Raster_Name " + str(the_Raster_Name))

            currentRaster_FileName = the_Raster_Name + ".tif"
            #self.debug_logger("ALERT 2.3b_pre currentRaster_FileName " + str(currentRaster_FileName))

            currentRaster_FileString = os.path.join(fileFolder_With_TRMM_Rasters ,currentRaster_FileName)
            #self.debug_logger("ALERT 2.3c_pre currentRaster_FileString " + str(currentRaster_FileString))

            currentOutputCopy_FullPathAndFile = os.path.join(path_to_extract_into, currentRaster_FileName)
            #self.debug_logger("ALERT 2.3d_pre currentOutputCopy_FullPathAndFile " + str(currentOutputCopy_FullPathAndFile))

            arcpy.CopyRaster_management(currentRaster_FileString, currentOutputCopy_FullPathAndFile)
            #self.debug_logger("ALERT 2.3e_pre Passed step, arcpy.CopyRaster_management")
                # This method only works with the S3 file name formats.. if there is a '.' in the file name, this functin breaks.
            #self._ksReplacement_CopyRaster_management(currentRaster_FileString, currentOutputCopy_FullPathAndFile)
            #self.debug_logger("ALERT 2.3e_pre Passed step, self._ksReplacement_CopyRaster_management")

            # Update, Add the spatial ref to the newly copied raster.
            # Define the coordinate system
            coor_system = "WGS 1984"
            sr = arcpy.SpatialReference(coor_system)
            arcpy.DefineProjection_management(currentOutputCopy_FullPathAndFile, sr)

            # Append the raster to the list!
            #extracted_raster_list.append(the_Raster_Name)
            extracted_raster_list.append(currentRaster_FileName) # Guess we need the extension on there too.. (and perhaps even the file location as well..)
            #self.debug_logger("ALERT 2.3f_pre Current For Loop Cycle Complete")


        #self.debug_logger("ALERT 2.4 extracted_raster_list " + str(extracted_raster_list))
        return extracted_raster_list


class TRMMCustomRasterCreator:

    """creates a custom raster from a given TRMMCustomRasterRequest object.

        raster_creator_options <dict>: config options for the TRMM raster creator.

            'workspace_fullpath' <str>: output workspace location for the raster creation process
            'remove_all_rasters_on_finish' <bool>: cleans up all raster output on finish.

            'archive_options' <dict>: local extracted rasters can be kept in the workspace to allow for faster processing in future runs

                'raster_name_prefix' <str>: given to differentiate between extracted rasters when deleting ex: "t_"
                'local_raster_archive_days' <int>: rasters outside this number will be deleted ex: 90
                'raster_name_datetime_format' <str>: to determine if a raster is outside the archive days,
                 each name must be in an easily convertable datetime string format. ex: "t_%Y%m%d%H"

            'debug_logger' <object.method>: method that will be passes variable string arguments to display current progress and values
            'exception_handler' <object.method>: method that will be variable string arguments with exception information
    """

    def __init__(self,  raster_creator_options):


        self.raster_creator_options = raster_creator_options

        # KS note, 2014-06-05 this is new!  This is the location of where TRMM Rasters are stored on the filesystem during the ETL process. (over there its called, RasterOutFolder)
        self.fileFolder_With_TRMM_Rasters = raster_creator_options['fileFolder_With_TRMM_Rasters']

        self.workspace_fullpath = raster_creator_options['workspace_fullpath']
        self.custom_raster_requests = []

        if not os.path.isdir(self.workspace_fullpath):
            os.mkdir(self.workspace_fullpath)

        self.debug_logger = raster_creator_options.get('debug_logger',lambda*a,**kwa:None)
        self.exception_handler = raster_creator_options.get('exception_handler',lambda*a,**kwa:None)

    def addCustomRasterReuests(self, custom_raster_requests):

        self.custom_raster_requests = custom_raster_requests

    def createCustomRasters(self):
        self.debug_logger("Starting TRMM Custom Raster Creation Process")

        try:
            arcpy.env.extent = arcpy.Extent(-180.0, -50.0, 180.0, 50.0) # max and min extent values a given TRMM raster
            arcpy.env.workspace = self.workspace_fullpath
            arcpy.env.overwriteOutput = True
            arcpy.CheckOutExtension("spatial")

            for custom_raster in self.custom_raster_requests:
                self.debug_logger("Processing Raster")

                factory_specifications = custom_raster.getFactorySpecifications()
                output_raster_fullpath = factory_specifications['output_raster_fullpath']
                raster_catalog_is_not_locked = arcpy.TestSchemaLock(custom_raster.getRasterCatalogFullpath())
                self.debug_logger("DEBUG: self.workspace_fullpath " + str(self.workspace_fullpath))


                #extracted_raster_list = custom_raster.extractRastersToWorkspace(self.workspace_fullpath)
                extracted_raster_list = custom_raster.extractRastersToWorkspace(self.workspace_fullpath, self.fileFolder_With_TRMM_Rasters)

                self.debug_logger("Len(extracted_raster_list) " + str(len(extracted_raster_list)))

                if extracted_raster_list and raster_catalog_is_not_locked:

                    final_raster = self._createCumulativeRaster(extracted_raster_list, factory_specifications)
                    self._saveRaster(final_raster, output_raster_fullpath, factory_specifications)

            self._finishCustomRasterManagment()
            self.debug_logger("Finished TRMM Custom Raster Creation Process")

        except Exception as e:

            self.debug_logger("==================== EXCEPTION ====================")
            self.debug_logger("System Error Message: " + str(e) + " | ArcPy Error Message: " + str(arcpy.GetMessages(2)))
            #self.exception_handler(dict(exception=str(e), messages=str(arcpy.GetMessages(2))))

        finally:
            arcpy.CheckInExtension("spatial")
            self.debug_logger("checked IN spatial extension")

    def _createCumulativeRaster(self, rasters_list, factory_specifications):

        self.debug_logger("Creating Cumulative Raster...")
        final_raster = sum([Con(IsNull(raster), 0, raster) for raster in rasters_list]) # for each raster in the list, set all NULL to 0 then SUM
        final_raster = Float(final_raster)
        final_raster = final_raster * 3 # multiply by 3 since each TRMM raster 3-hour period is an average not a sum

##        if factory_specifications.get('clip_extent', None):
##
##            self.debug_logger("Adding Clip Extent...")
##            output_clip_raster = os.path.join(os.path.join(sys.path[0], "scratch.gdb"),"temp_clip")
##            final_raster = arcpy.Clip_management(final_raster, factory_specifications['clip_extent'], output_clip_raster)
##
##        elif factory_specifications.get('clip_raster', None):
##
##            self.debug_logger("Adding Clip Raster...")
##            final_raster = final_raster * Raster(factory_specifications['clip_raster'])

        final_raster = SetNull(final_raster == 0, final_raster) # set 0's back to NULL after all mathematical operations are peformed
        self.debug_logger("SetNull(final_raster == 0, final_raster)")

        return final_raster

    def _saveRaster(self, raster_to_save, output_raster_fullpath, factory_specifications):
        self.debug_logger("_saveRaster: Alert 1: raster_to_save " + str(raster_to_save))
        self.debug_logger("_saveRaster: Alert 2: raster_to_save " + str(output_raster_fullpath))
        self.debug_logger("_saveRaster: Alert 3: raster_to_save " + str(factory_specifications))

        self.debug_logger("Saving Final Raster")

        if factory_specifications.get('AddColormap_management_config', None):
            self.debug_logger("Adding Color Map...")

            color_map_config = factory_specifications['AddColormap_management_config']
            r = arcpy.AddColormap_management(raster_to_save, color_map_config.get('in_template_raster',''), color_map_config['input_CLR_file'])
            self.debug_logger("AddColormap_management Result " + str(r.status))

        raster_name = os.path.basename(output_raster_fullpath)
        raster_to_save.save(raster_name)
        local_raster_fullpath = os.path.join(self.workspace_fullpath, raster_name)
        self.debug_logger("local_raster_fullpath " + str(local_raster_fullpath))
        self.debug_logger("output_raster_fullpath " + str(output_raster_fullpath))

        self._removeExistingRasterIfExists(output_raster_fullpath)
        self._copyRaster(factory_specifications['CopyRaster_management_config'], local_raster_fullpath, output_raster_fullpath)
        # Comment the next line to see the output raster work in progress.
        #self._removeExistingRasterIfExists(local_raster_fullpath)

    def _copyRaster(self, copy_raster_managment_config, local_raster_fullpath, output_raster_fullpath):

        self.debug_logger("Copying Raster..." + str(output_raster_fullpath))
        r = arcpy.CopyRaster_management(local_raster_fullpath, output_raster_fullpath,
            copy_raster_managment_config.get('config_keyword',''), copy_raster_managment_config.get('background_value',''),
            copy_raster_managment_config.get('nodata_value',''), copy_raster_managment_config.get('onebit_to_eightbit',''),
            copy_raster_managment_config.get('colormap_to_RGB',''), copy_raster_managment_config.get('pixel_type','')
        )
        self.debug_logger("CopyRaster_management Result"+str(r.status))

    def _removeExistingRasterIfExists(self, output_raster_fullpath):

        if arcpy.Exists(output_raster_fullpath):

            self.debug_logger("Deleting..." + str(output_raster_fullpath))
            r = arcpy.Delete_management(output_raster_fullpath)
            self.debug_logger("Delete_management Result" + str(r.status))

    def _finishCustomRasterManagment(self):
        self.debug_logger("Finishing Custom Raster Creation")

        archive_options = self.raster_creator_options.get('archive_options', None)
        remove_all_rasters_on_finish = self.raster_creator_options.get('remove_all_rasters_on_finish', False)

        if archive_options and not remove_all_rasters_on_finish:

            raster_name_prefix = archive_options.get('raster_name_prefix', None)
            archive_days = archive_options.get('local_raster_archive_days', None)
            raster_name_datetime_format = archive_options.get('raster_name_datetime_format', None)

            if (raster_name_prefix and archive_days and raster_name_datetime_format):

                archive_date = datetime.utcnow() - timedelta(days=archive_days)
                local_raster_list = [r for r in arcpy.ListRasters(raster_name_prefix+"*","*") if str(r[:len(raster_name_prefix)]).lower() == str(raster_name_prefix)]
                list_of_rasters_to_delete = [raster for raster in local_raster_list if datetime.strptime(str(raster), raster_name_datetime_format) < archive_date]
                self._deleteRasters(list_of_rasters_to_delete)

        elif remove_all_rasters_on_finish:

            self.debug_logger("Removing All Rasters In Local Workspace...")
            self._deleteRasters(arcpy.ListRasters("*"))

    def _deleteRasters(self, list_of_rasters_to_delete):

        for r in list_of_rasters_to_delete:
            arcpy.Delete_management(r)
