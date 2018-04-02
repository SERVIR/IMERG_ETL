# Developer: Daniel Duarte
# Company:   Spatial Development International
# E-mail:    dduarte@spatialdev.com


# standard library
from datetime import datetime, timedelta
import os

# third-party
import arcpy


class FileGeoDatabase(object):
    
    """
        Wrapper class for a file geodatabase.
    """
    
    def __init__(self, basepath, name, options=None):
        
        if not options:
            options = {}
                 
        self.name = name
        self.basepath = basepath
        self.version = options.get('version', "CURRENT")
        self.options = options
        self.fullpath = os.path.join(basepath, name)
        
        if not arcpy.Exists(self.fullpath):
            arcpy.CreateFileGDB_management(self.basepath, self.name, self.version)
            
    def compactFileGeoDatabase(self):
        
        compact_interval_days = self.options.get('compact_interval_days', None)
        compact_ready = compact_interval_days and datetime.utcnow().day % compact_interval_days == 0
        
        if compact_ready:
            arcpy.Compact_management(self.fullpath)


class ArcTableUtils(object):
    
    """
        Utility object that contains common arcpy.<cursor_type> operations and patterns.
    """
                    
    def updateFields(self, table_fullpath, fields_dict, options):
        
        """
            This method updates the fields for the given table_fullpath.
            
            arguments:
            
                table_fullpath <str>: fullpath to a given table object (raster catalog, feature class, table)
                fields_dict <dict>: contains key-value (field_name, field_value) pairs that will be used to udpate the given table_fullpath
                options <dict>: options for arcpy.UpdateCursor
        """
        
        rows = arcpy.UpdateCursor(table_fullpath, options.get('where_clause',''), options.get('spatial_reference',''),options.get('fields',''), options.get('sort_fields',''))
        try: 
            for row in rows:
                for field_name in fields_dict:
                    print "updating field...", str(field_name), fields_dict[field_name]
                    row.setValue(str(field_name), fields_dict[field_name])
                rows.updateRow(row)
        finally:
            del rows
            
    def deleteOutdatedRows(self, table_fullpath, archive_limit, date_column_name, datetime_field_format, datetime_sql_cast):
                
        """
            This method deletes all rows from the given table_fullpath that are outside of the given archive_limit.
            
            arguments:
            
                table_fullpath <str>: fullpath to a given table object (raster catalog, feature class, table)
                archive_limit <int>: the number of days minus todays date to delete from the given table_fullpath
                date_column_name <str>: the name of the column that contains the date values for the table_fullpath.
                datetime_field_format <str>: the format (ex: %Y%m%d%H') of the datetime values in the given date_column_name.
                datetime_sql_cast <str>: the datetime CAST operator associated with the underlying SQL type to use to create a datetime object 
                in the SQL WHERE clause.
        """
        
        archive_limit_date = (datetime.utcnow() - timedelta(days=int(archive_limit))).strftime(datetime_field_format)
        where_clause = "%s <= %s \'%s\'" % (date_column_name, datetime_sql_cast, archive_limit_date)
        rows = arcpy.UpdateCursor(table_fullpath, where_clause,"", date_column_name)
        
        try:
            for row in rows:
                rows.deleteRow(row)
        finally:
            del rows
            
    def getValuesFromField(self, table_fullpath, where_clause, field): 
        
        """
            This method returns the values from a given field.
            
            arguments:
            
                table_fullpath <str>: fullpath to a given table object (raster catalog, feature class, table)
                where_clause <str>: the conditions of the query
                field <str>: the column name in the table_fullpath to retireve the values from
        """
        
        try:
            rows = arcpy.SearchCursor(table_fullpath, where_clause, "", field, "")
            return [str(row.getValue(field)) for row in rows]
        finally:
            del rows


class ArcTable(object):
    
    """
        Utility object that contains common operations and patterns performed on feature classes, raster catalogs, tables.
    """
    
    def __init__(self, out_path, out_name, options):
        
        self.name = out_name
        self.basepath = out_path
        self.fullpath = os.path.join(out_path, out_name)
        self.options = options
        self.arc_table_utils = ArcTableUtils()
        
    def delete(self):
        if arcpy.Exists(self.fullpath):
            arcpy.Delete_management(self.fullpath)
    
    def listFields(self):
        return arcpy.ListFields(self.fullpath)
                
    def deleteOutdatedRows(self):
        self.arc_table_utils.deleteOutdatedRows(
            self.fullpath, self.options['archive_days'], self.options['datetime_field'], 
            self.options['datetime_field_format'], self.options['datetime_sql_cast']
        )
        
    def getValuesFromDatetimeRange(self, data_field, start_datetime, end_datetime, additional_where_clause=""): 
        
        """
            This method retrieves values from the given datetime range from the 'fullpath' associated with the object that is inheriting ArcTable.
            
            arguments:
            
                data_field <str>: the name of the field to retireve the values from 
                start_datetime <datetime.datetime>: retrieve all values before this date
                end_datetime <datetime.datetime>: retrieve all values after this date
                additional_where_clause <str>: optional conditions for the WHERE clause
        """

        start_datetime = start_datetime.strftime(self.options['datetime_field_format'])
        end_datetime = end_datetime.strftime(self.options['datetime_field_format'])
        datetime_sql_cast = self.options['datetime_sql_cast'] # this is important if the underlying SQL type changes
        datetime_field = self.options['datetime_field']
        
        where_clause = "%s <= %s \'%s\'" % (datetime_field, datetime_sql_cast, start_datetime)
        where_clause += "AND %s >= %s \'%s\'" % (datetime_field, datetime_sql_cast, end_datetime)
        where_clause += additional_where_clause # this is optional. It is available for specific queries that do not only contain a datetime range.
        print "where_clause",where_clause
        
        return self.arc_table_utils.getValuesFromField(self.fullpath, where_clause, data_field)
    
    def updateFields(self, fields_dict, options):
        self.arc_table_utils.updateFields(self.fullpath, fields_dict, options)
        
    def updateFieldsForInput(self, input_name, fields_dict, input_field_name="Name"):
        
        """
            This method updates the fields for the given input_name associated with the given input_field_name (column name in table). 
            For example, if input_name was a raster then it will update the fields associated with the given raster with the given fields_dict.
        """
        
        where_clause = "%s = \'%s\'" % (input_field_name, input_name)
        self.arc_table_utils.updateFields(self.fullpath, fields_dict, {'where_clause':where_clause})
        

class FeatureClass(ArcTable):
    
    """
        Wrapper class for a feature class.
    """
    
    def __init__(self, out_path, out_name, options): 
        ArcTable.__init__(self, out_path, out_name, options)
        
        if not arcpy.Exists(self.fullpath):
            
            arcpy.CreateFeatureclass_management(out_path, out_name, 
                self.options.get('geometry_type',''), self.options.get('template',''),self.options.get('has_m',''), 
                self.options.get('has_z',''), self.options.get('spatial_reference',''), self.options.get('config_keyword', ''), 
                self.options.get('spatial_grid_1',''), self.options.get('spatial_grid_2',''), self.options.get('spatial_grid_3','')
            )

class RasterCatalog(ArcTable):
    
    """
        Wrapper class for a raster catalog.
    """
    
    def __init__(self, raster_catalog_basepath, raster_catalog_name, options):
        ArcTable.__init__(self, raster_catalog_basepath, raster_catalog_name, options)
        
        if not arcpy.Exists(self.fullpath):

            arcpy.CreateRasterCatalog_management(raster_catalog_basepath, raster_catalog_name, 
                options.get('raster_spatial_reference',''), options.get('spatial_reference',''), options.get('config_keyword',''), options.get('spatial_grid_1',''), 
                options.get('spatial_grid_2',''), options.get('spatial_grid_3',''), options.get('raster_management_type',''), options.get('template_raster_catalog','')
            )
           
class AGServiceManager(object):
    
    """
        Object that manages operations performed on ArcGIS services.
        
        arguments:
        
            services <str/list>: the name of the services to manage. A single service can be given as a string. Multiple
            services must be placed in a list.
            path_to_agssom <str>: the fullpath to the .exe that executes the given operations
            server <str>: the server ip (or 'localhost') that hosts the ArcGIS services
        
        These operations on services include:
        
            - starting
            - stopping
            - stoping then starting
            - any of the above for all services 
    """

    def __init__(self, services, path_to_agssom, server):
        
        if isinstance(services, str):
            services = [services]
            
        self.services = services
        self.path_to_agssom = path_to_agssom
        self.server = server
    
    def stopService(self):
        
        for service in self.services:
            self.toCommandLine("%s %s -x %s" % (self.path_to_agssom, self.server, service)) 
            
    def startService(self):
        
        for service in self.services:
            self.toCommandLine("%s %s -s %s" % (self.path_to_agssom, self.server, service)) 
        
    def refreshService(self):
        
        for service in self.services:
            self.toCommandLine("%s %s -r %s" % (self.path_to_agssom, self.server, service)) 
    
    def refreshAllServices(self):
        self.toCommandLine("%s %s -r *all*" % (self.path_to_agssom, self.server))
    
    def stopAllServices(self):
        self.toCommandLine("%s %s -x *all*" % (self.path_to_agssom, self.server))
        
    def startAllServices(self):
        self.toCommandLine("%s %s -s *all*" % (self.path_to_agssom, self.server))
            
    def toCommandLine(self, command):
        os.system(command)
