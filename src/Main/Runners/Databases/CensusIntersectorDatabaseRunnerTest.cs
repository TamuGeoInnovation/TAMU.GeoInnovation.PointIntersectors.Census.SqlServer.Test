using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Threading;
using System.Diagnostics;
using System.ComponentModel;

using USC.GISResearchLab.Common.Databases.SchemaManagers;
using USC.GISResearchLab.Common.Databases.TypeConverters;
using USC.GISResearchLab.Common.Databases.DataTables;
using USC.GISResearchLab.Common.Utils.Databases.TableDefinitions;
using USC.GISResearchLab.Common.FieldMappings;
using USC.GISResearchLab.Common.Databases.FieldMappings;

using USC.GISResearchLab.Common.Databases.Runners.AbstractClasses;
using USC.GISResearchLab.Census.Runners.Queries.Options;
using USC.GISResearchLab.Common.Utils.Databases;

using USC.GISResearchLab.Common.Diagnostics.TraceEvents;
using USC.GISResearchLab.Common.Threading.ProgressStates;

using USC.GISResearchLab.Common.Databases.DataReaders;
using System.Data.SqlClient;
using USC.GISResearchLab.Common.Core.Threading.ThreadPoolWaits;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using TAMU.GeoInnovation.PointIntersectors.Census.PointIntersecters.Interfaces;
using USC.GISResearchLab.Common.Core.Databases;
using TAMU.GeoInnovation.PointIntersectors.Census.OutputData.CensusRecords;
using USC.GISResearchLab.AddressProcessing.Core.Standardizing.StandardizedAddresses.Lines.LastLines;
using TAMU.GeoInnovation.PointIntersectors.Census.SqlServer.Census2010;
//using USC.GISResearchLab.Common.Databases.Panels.DatabaseTableChoosers;

namespace USC.GISResearchLab.Census.Runners.Databases
{



    public class CensusIntersectorDatabaseRunnerTest : AbstractTraceableBackgroundWorkableWebStatusReportableDatabaseRunner
    {
        public List<string> Grid_Densities = new List<string>(new string[] { "LOW", "MEDIUM", "HIGH" });

        public string Level2GridDensity;

        public string Level3GridDensity;

        public string Level4GridDensity;

        public bool GeometryIndexSelected;

        public bool GeographyIndexSelected;

        #region Properties

        public ICensusPointIntersector CensusPointIntersector
        {
            get;
            set;
        }

        #endregion

        #region Constructors

        public CensusIntersectorDatabaseRunnerTest()
            : base()
        {
            RunnerName = "AggieCensusIntersector";
        }

        public CensusIntersectorDatabaseRunnerTest(TraceSource traceSource)
            : base(traceSource)
        {
            RunnerName = "AggieCensusIntersector";
        }

        public CensusIntersectorDatabaseRunnerTest(BackgroundWorker backgroundWorker)
            : base(backgroundWorker)
        {
            RunnerName = "AggieCensusIntersector";
        }

        public CensusIntersectorDatabaseRunnerTest(BackgroundWorker backgroundWorker, TraceSource traceSource)
            : base(backgroundWorker, traceSource)
        {
            RunnerName = "AggieCensusIntersector";
        }


        #endregion

        #region GetWorkFunctions

        public override IDataReader GetWorkAsDataReader(bool shouldOpenClose)
        {
            IDataReader ret = null;
            try
            {

                if (shouldOpenClose)
                {
                    DBManagerInputData.Open();
                }

                BatchIntersect args = (BatchIntersect)BatchDatabaseOptions;

                string sql = "SELECT ";
                sql += " " + DatabaseUtils.AsDbColumnName(args.FieldId) + " " + " AS [Id], ";
                sql += " " + DatabaseUtils.AsDbColumnName(args.FieldLatitude) + " " + " AS [Latitude], ";
                sql += " " + DatabaseUtils.AsDbColumnName(args.FieldLongitude) + " " + " AS [Longitude], ";
                sql += " " + DatabaseUtils.AsDbColumnName(args.FieldState) + " " + " AS [State] ";
                sql += " FROM " + DatabaseUtils.AsDbTableName(args.Table, true) + " ";

                sql += " WHERE ";

                sql += " (" + DatabaseUtils.AsDbColumnName(args.FieldId) + " is not null)";

                if (BatchDatabaseOptions.NonProcessedOnly || BatchDatabaseOptions.ShouldFilterInputData)
                {


                    if (args.NonProcessedOnly)
                    {
                        sql += " AND ";

                        DatabaseFieldMapping updatedField = args.OutputBookKeepingFieldMappings.GetFieldMapping("processed");
                        if (updatedField != null)
                        {
                            string updatedFieldName = updatedField.Value;

                            sql += " (" + DatabaseUtils.AsDbColumnName(updatedFieldName) + " <> 1 or  " + DatabaseUtils.AsDbColumnName(updatedFieldName) + " is null)";

                        }
                    }

                    if (BatchDatabaseOptions.ShouldFilterInputData)
                    {
                        sql += " AND ";

                        sql += " (" + DatabaseUtils.AsDbColumnName(BatchDatabaseOptions.FieldFilterField) + " " + BatchDatabaseOptions.FieldFilterValue + " )";
                    }


                }

                if (args.ShouldOrderWorkByIdField)
                {
                    sql += " ORDER BY " + DatabaseUtils.AsDbColumnName(args.FieldId) + " ASC";
                }

                ret = DBManagerInputData.ExecuteReader(CommandType.Text, sql, false);
            }
            catch (ThreadAbortException te)
            {
                throw te;
            }
            catch (Exception e)
            {
                if (DBManagerInputData != null)
                {
                    if (DBManagerInputData.Connection != null)
                    {
                        if (DBManagerInputData.Connection.State != ConnectionState.Closed)
                        {
                            if (!DBManagerInputDataClosed)
                            {
                                DBManagerInputData.Connection.Close();
                                DBManagerInputDataClosed = true;
                            }
                        }
                    }

                    DBManagerInputData.Dispose();
                    DBManagerInputData = null;
                }

                throw new Exception("Error occured getting work: " + e.Message, e);
            }
            finally
            {
                if (shouldOpenClose)
                {
                    if (DBManagerInputData != null)
                    {
                        if (DBManagerInputData.Connection != null)
                        {
                            if (DBManagerInputData.Connection.State != ConnectionState.Closed)
                            {
                                if (!DBManagerInputDataClosed)
                                {
                                    DBManagerInputData.Connection.Close();
                                    DBManagerInputDataClosed = true;
                                }
                            }
                        }

                        DBManagerInputData.Dispose();
                        DBManagerInputData = null;
                    }
                }
            }
            return ret;
        }

        #endregion

        public override bool ProcessRecords(DataTable dataTable)
        {
            return ProcessRecords(dataTable.CreateDataReader());
        }
        public override bool ProcessRecords(IDataReader dataReader)
        {
            bool ret = false;
            /* Generate the Geography Databse indexes on all the tables involved */

            BatchIntersectTest args = (BatchIntersectTest)BatchDatabaseOptions;

            bool firstRecord = true;


            try
            {

                if (args.ShouldLeaveDatabaseConnectionOpen)
                {
                    if (DBManagerInputDataUpdate.Connection.State != ConnectionState.Open)
                    {
                        DBManagerInputDataUpdate.Connection.Open();
                    }
                }



                if (dataReader != null)
                {

                    while (dataReader.Read())
                    {
                        if (!ShouldStop)
                        {
                            for (int i = 0; i < 3; i++)
                            {
                                for (int j = 0; j < 3; j++)
                                {
                                    for (int k = 0; k < 3; k++)
                                    {
                                        for (int l = 0; l < 3; l++)
                                        {
                                            if
                                                (IsIndexSelected(Grid_Densities[i] + Grid_Densities[j] + Grid_Densities[k] + Grid_Densities[l], args.SelectedIndices)
                                                    || args.AllIndices)
                                            {
                                                string id = DatabaseUtils.StringIfNull(dataReader["Id"]);
                                                double longitude = DatabaseUtils.DoubleIfNull(dataReader["longitude"]);
                                                double latitude = DatabaseUtils.DoubleIfNull(dataReader["latitude"]);
                                                string temp_state = DatabaseUtils.StringIfNull(dataReader["state"]);


                                                string state = (string)StateUtils.HashTable_FIPS_States_Abbreviations[temp_state];

                                                double xmin = -90, xmax = 90, ymin = -90, ymax = 90;
                                                if (StateUtils.isState(state))
                                                {
                                                    if (firstRecord)
                                                    {


                                                        if (CensusPointIntersector != null)
                                                        {

                                                            if (args.IsInitialRun && args.GeographyIndexSelected)
                                                            {
                                                                ((SqlServerCensus2010PointIntersectorTest)CensusPointIntersector).GenerateIndices(false, state + "_tabblock10", 0, 0, 0, 0, args.CellsPerObject);
                                                                //CensusPointIntersector.GenerateIndices(false, "us_county10", 0, 0, 0, 0, args.CellsPerObject);
                                                                ((SqlServerCensus2010PointIntersectorTest)CensusPointIntersector).GenerateIndices(false, state + "_place10", 0, 0, 0, 0, args.CellsPerObject);
                                                                ((SqlServerCensus2010PointIntersectorTest)CensusPointIntersector).GenerateIndices(false, state + "_cousub10", 0, 0, 0, 0, args.CellsPerObject);
                                                                //CensusPointIntersector.GenerateIndices(false, "us_MetDiv10", 0, 0, 0, 0, args.CellsPerObject);
                                                                //CensusPointIntersector.GenerateIndices(false, "us_cbsa10", 0, 0, 0, 0, args.CellsPerObject);
                                                            }

                                                            else if (args.IsInitialRun && args.GeometryIndexSelected)
                                                            {

                                                                DataTable dataTable = CensusPointIntersector.GetBoundaryPoints("[Tiger2010CensusBlocks].[dbo]." + state + "_tabblock10");


                                                                if (dataTable != null && dataTable.Rows.Count > 0)
                                                                {
                                                                    xmin = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MinX"]);
                                                                    ymin = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MinY"]);
                                                                    xmax = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MaxX"]);
                                                                    ymax = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MaxY"]);
                                                                }

                                                                ((SqlServerCensus2010PointIntersectorTest)CensusPointIntersector).GenerateIndices(true, state + "_tabblock10", xmin, ymin, xmax, ymax, args.CellsPerObject);




                                                                //dataTable = CensusPointIntersector.GetBoundaryPoints("[Tiger2010CountryFiles].[dbo].[us_county10]");


                                                                //if (dataTable != null && dataTable.Rows.Count > 0)
                                                                //{
                                                                //    xmin = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MinX"]);
                                                                //    ymin = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MinY"]);
                                                                //    xmax = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MaxX"]);
                                                                //    ymax = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MaxY"]);
                                                                //}
                                                                //CensusPointIntersector.GenerateIndices(true, "us_county10", xmin, ymin, xmax, ymax, args.CellsPerObject);

                                                                dataTable = CensusPointIntersector.GetBoundaryPoints("[Tiger2010StateFiles].[dbo]." + state + "_place10");


                                                                if (dataTable != null && dataTable.Rows.Count > 0)
                                                                {
                                                                    xmin = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MinX"]);
                                                                    ymin = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MinY"]);
                                                                    xmax = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MaxX"]);
                                                                    ymax = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MaxY"]);
                                                                }

                                                                ((SqlServerCensus2010PointIntersectorTest)CensusPointIntersector).GenerateIndices(true, state + "_place10", xmin, ymin, xmax, ymax, args.CellsPerObject);

                                                                dataTable = CensusPointIntersector.GetBoundaryPoints("[Tiger2010StateFiles].[dbo]." + state + "_cousub10");


                                                                if (dataTable != null && dataTable.Rows.Count > 0)
                                                                {
                                                                    xmin = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MinX"]);
                                                                    ymin = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MinY"]);
                                                                    xmax = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MaxX"]);
                                                                    ymax = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MaxY"]);
                                                                }

                                                                ((SqlServerCensus2010PointIntersectorTest)CensusPointIntersector).GenerateIndices(true, state + "_cousub10", xmin, ymin, xmax, ymax, args.CellsPerObject);



                                                                //dataTable = CensusPointIntersector.GetBoundaryPoints("[Tiger2010CountryFiles].[dbo].[us_MetDiv10]");


                                                                //if (dataTable != null && dataTable.Rows.Count > 0)
                                                                //{
                                                                //    xmin = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MinX"]);
                                                                //    ymin = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MinY"]);
                                                                //    xmax = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MaxX"]);
                                                                //    ymax = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MaxY"]);
                                                                //}

                                                                //CensusPointIntersector.GenerateIndices(true, "us_MetDiv10", xmin, ymin, xmax, ymax, args.CellsPerObject);

                                                                //dataTable = CensusPointIntersector.GetBoundaryPoints("[Tiger2010CountryFiles].[dbo].[us_cbsa10]");


                                                                //if (dataTable != null && dataTable.Rows.Count > 0)
                                                                //{
                                                                //    xmin = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MinX"]);
                                                                //    ymin = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MinY"]);
                                                                //    xmax = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MaxX"]);
                                                                //    ymax = DatabaseUtils.DoubleIfNull(dataTable.Rows[0]["MaxY"]);
                                                                //}

                                                                //CensusPointIntersector.GenerateIndices(true, "us_cbsa10", xmin, ymin, xmax, ymax, args.CellsPerObject);
                                                            }

                                                        }
                                                        /*donot include this   //double xmin = -90, xmax = 90, ymin = -90, ymax = 90;
                                                           //try
                                                           //{
                                                           //    if (args.ShouldLeaveDatabaseConnectionOpen)
                                                           //    {
                                                           //        if (DBManagerInputDataUpdate.Connection.State != ConnectionState.Open)
                                                           //        {
                                                           //            DBManagerInputDataUpdate.Connection.Open();
                                                           //        }
                                                           //    }
                                                           //    if (dataReader != null)
                                                           //    {
                                                           //        while (dataReader.Read())
                                                           //        {
                                                           //            if (!ShouldStop)
                                                           //            {
                                                           //                string id = DatabaseUtils.StringIfNull(dataReader["Id"]);
                                                           //                double longitude = DatabaseUtils.DoubleIfNull(dataReader["longitude"]);
                                                           //                double latitude = DatabaseUtils.DoubleIfNull(dataReader["latitude"]);
                                                           //                string state = DatabaseUtils.StringIfNull(dataReader["state"]);


                                                           //                if (!String.IsNullOrEmpty(id))
                                                           //                {
                                                           //                    if (xmin > longitude)
                                                           //                    {
                                                           //                        xmin = longitude;
                                                           //                    }
                                                           //                    if (xmax < longitude)
                                                           //                    {
                                                           //                        xmax = longitude;
                                                           //                    }
                                                           //                    if (ymin > latitude)
                                                           //                    {
                                                           //                        xmin = latitude;
                                                           //                    }
                                                           //                    if (xmax < latitude)
                                                           //                    {
                                                           //                        xmax = latitude;
                                                           //                    }

                                                           //                }
                                                           //            }
                                                           //        }
                                                           //    }
                                                           //}
                                                           //catch (Exception e)
                                                           //{
                                                           //    Console.WriteLine("The mentioned exception occurred while trying to calculate the xmin, ymin,xmax and ymax" + e.Message);
                                                           //} */
                                                        firstRecord = false;
                                                    }
                                                }
                                                if (!String.IsNullOrEmpty(id) && longitude != 0 && latitude != 0)
                                                {

                                                    object[] record = new object[3];
                                                    record[0] = longitude;
                                                    record[1] = latitude;
                                                    record[2] = state;

                                                    if (TraceSource != null)
                                                    {
                                                        TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Running, "{0}: {1} {2} {3}", new object[] { id, longitude, latitude, state });
                                                    }

                                                    CensusRecord result = (CensusRecord)ProcessRecord(id, record, Grid_Densities[i], Grid_Densities[j], Grid_Densities[k], Grid_Densities[l], xmin, ymin, xmax, ymax, args.GeometryIndexSelected);

                                                    if (!ShouldStop)
                                                    {
                                                        UpdateRecord(id, result, Grid_Densities[i], Grid_Densities[j], Grid_Densities[k], Grid_Densities[l]);

                                                        if (TraceSource != null)
                                                        {
                                                            TraceSource.TraceEvent(TraceEventType.Information, (int)ProcessEvents.Running, " - {0}, {1}, {2}, {3}, {4}, {5}, {6}",
                                                             new object[] { 
                                                                                "200", 
                                                                                "Success", 
                                                                                result.Block, 
                                                                                result.BlockGroup,
                                                                                result.Tract,
                                                                                result.CountyFips, 
                                                                                result.StateFips, 
                                                        });
                                                        }

                                                        object[] data = new object[1];
                                                        data[0] = result;

                                                        RecordsCompleted++;
                                                        UpdateProcessingStatus(RecordsTotal, RecordsCompleted, data);
                                                    }
                                                    else
                                                    {
                                                        SignalCancelled();
                                                        ret = false;
                                                        break;
                                                    }
                                                }
                                                else
                                                {
                                                    RecordsCompleted++;
                                                    UpdateProcessingStatus(RecordsTotal, RecordsCompleted);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            SignalCancelled();
                            ret = false;
                            break;
                        }
                    }

                    ret = true;

                }

                if (!ShouldStop)
                {
                    UpdateProcessingStatusFinished();
                }
                else
                {
                    SignalCancelled();
                    ret = false;
                }

            }
            catch (Exception e)
            {
                UpdateProcessingStatusAborted(e);

                if (dataReader != null)
                {
                    if (!dataReader.IsClosed)
                    {
                        dataReader.Close();
                    }
                }

                if (args.ShouldLeaveDatabaseConnectionOpen)
                {
                    try
                    {
                        if (DBManagerInputDataUpdate.Connection != null)
                        {
                            if (DBManagerInputDataUpdate.Connection.State != ConnectionState.Closed)
                            {
                                DBManagerInputDataUpdate.Connection.Close();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new Exception("Exception closing input database: " + ex.Message, ex);
                    }
                }

                throw new Exception("Error occured processing records: " + e.Message, e);
            }
            finally
            {
                if (dataReader != null)
                {
                    if (!dataReader.IsClosed)
                    {
                        dataReader.Close();
                    }
                }

                if (args.ShouldLeaveDatabaseConnectionOpen)
                {
                    try
                    {
                        if (DBManagerInputDataUpdate.Connection != null)
                        {
                            if (DBManagerInputDataUpdate.Connection.State != ConnectionState.Closed)
                            {
                                DBManagerInputDataUpdate.Connection.Close();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new Exception("Exception closing input database: " + ex.Message, ex);
                    }
                }
            }

            return ret;
        }

        public bool UpdateRecord(object recordId, object result, string Level1, string Level2, string Level3, string Level4)// overload it to take the index name as well
        {




            bool ret = false;

            CensusRecord censusRecord = null;
            try
            {
                censusRecord = (CensusRecord)result;
                BatchIntersect BatchOptions = (BatchIntersect)BatchDatabaseOptions;

                string sql = "";

                // for SqlServer, first turn off warnings so fields are truncated if neccessary
                if (DBManagerInputDataUpdate.DatabaseType == DatabaseType.SqlServer)
                {
                    sql = " SET ANSI_WARNINGS OFF; ";
                }

                sql = " UPDATE " + DatabaseUtils.AsDbTableName(BatchOptions.Table, true) + " SET ";

                sql += "   " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "CensusYear") + "=@FieldCYear ";
                sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "Value") + "=@FieldCBlock ";
                sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "BlockGroup") + "=@FieldCBlockGroup ";
                sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "Tract") + "=@FieldCTract ";
                sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "CountyFips") + "=@FieldCCountyFips ";
                sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "PlaceFips") + "=@FieldCPlaceFips ";
                sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "MSAFips") + "=@FieldCMSAFips ";
                sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "MCDFips") + "=@FieldCMCDFips ";
                sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "CBSAFips") + "=@FieldCCBSAFips ";
                sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "CBSAMicro") + "=@FieldCBSAMicro ";
                sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "MetDivFips") + "=@FieldCMetDivFips ";
                sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "StateFips") + "=@FieldCStateFips ";

                sql += " WHERE " + BatchOptions.FieldId + "=@ID ";

                SqlCommand cmd = new SqlCommand(sql);

                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("FieldCYear", SqlDbType.VarChar, censusRecord.CensusYear.ToString()));//Intead of the string here, use the prefix code which u want your columns to be created with
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("FieldCBlock", SqlDbType.VarChar, censusRecord.Block));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("FieldCBlockGroup", SqlDbType.VarChar, censusRecord.BlockGroup));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("FieldCTract", SqlDbType.VarChar, censusRecord.Tract));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("FieldCCountyFips", SqlDbType.VarChar, censusRecord.CountyFips));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("FieldCPlaceFips", SqlDbType.VarChar, censusRecord.PlaceFips));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("FieldCMSAFips", SqlDbType.VarChar, censusRecord.MsaFips));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("FieldCMCDFips", SqlDbType.VarChar, censusRecord.McdFips));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("FieldCCBSAFips", SqlDbType.VarChar, censusRecord.CbsaFips));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("FieldCBSAMicro", SqlDbType.VarChar, censusRecord.CbsaMicro));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("FieldCMetDivFips", SqlDbType.VarChar, censusRecord.MetDivFips));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("FieldCStateFips", SqlDbType.VarChar, censusRecord.StateFips));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("ID", SqlDbType.VarChar, recordId));

                DBManagerInputDataUpdate.AddParameters(cmd.Parameters);

                // for Test framework, I am not executing the sql command filling entries for census fields associalted with each shape entry and hence the same is not currently supported

                //DBManagerInputDataUpdate.ExecuteNonQuery(CommandType.Text, cmd.CommandText, !BatchOptions.ShouldLeaveDatabaseConnectionOpen);


                if (true)
                {
                    try
                    {
                        sql = "";

                        // for SqlServer, first turn off warnings so fields are truncated if neccessary
                        if (DBManagerInputDataUpdate.DatabaseType == DatabaseType.SqlServer)
                        {
                            sql = " SET ANSI_WARNINGS OFF; ";
                        }

                        sql += " UPDATE " + DatabaseUtils.AsDbTableName(BatchOptions.Table, true) + " SET ";

                      

                        /* Eliminating all the redundant output columns except the TimeTaken since that is the only one needed*/

                        //sql += "   " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "Version") + "=@FieldVersion ";
                        sql += "  " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "TimeTaken") + "=@FieldTimeTaken ";
                        //sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "TransactionId") + "=@FieldTransactionId ";
                        //sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "Source") + "=@FieldSource ";
                        //sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "ErrorMessage") + "=@FieldErrorMessage";
                        //sql += " , " + DatabaseUtils.AsDbColumnName(Level1 + Level2 + Level3 + Level4 + "Processed") + "=@FieldProcessed ";

                        sql += " WHERE " + BatchOptions.FieldId + "=@ID ";

                        cmd = new SqlCommand(sql);

                        /* Eliminating all the redundant output columns except the TimeTaken*/

                        
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("FieldTimeTaken", SqlDbType.Decimal, censusRecord.TimeTaken));
                        
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("ID", SqlDbType.VarChar, recordId));

                        DBManagerInputDataUpdate.AddParameters(cmd.Parameters);


                        DBManagerInputDataUpdate.ExecuteNonQuery(CommandType.Text, cmd.CommandText, !BatchOptions.ShouldLeaveDatabaseConnectionOpen);
                    }
                    catch (Exception ex)
                    {
                        throw new Exception("Exception updating book keeping fields: " + ex.Message, ex);
                    }
                }

                ret = true;
            }
            catch (ThreadAbortException te)
            {
                throw te;
            }
            catch (Exception e)
            {
                if (BatchDatabaseOptions.AbortOnError)
                {
                    throw new Exception("Error occured updating record: " + recordId + " : " + e.Message, e);
                }
                else
                {
                    ErrorCount++;
                }
            }

            return ret;
        }


        public object ProcessRecord(object recordId, object record, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeometryIndexSelected)
        {

            object ret = false;
            string added = DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss");
            string transactionGuid = Guid.NewGuid().ToString();

            try
            {
                object[] recordFields = (object[])record;
                double lon = (double)recordFields[0];
                double lat = (double)recordFields[1];
                string state = (string)recordFields[2];

                if (CensusPointIntersector != null)
                {
                    ret = ((SqlServerCensus2010PointIntersectorTest)CensusPointIntersector).GetRecord(lon, lat, state, Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax, IsGeometryIndexSelected);
                }
                else
                {
                    throw new Exception("Census intersector is null");
                }
            }
            catch (ThreadAbortException te)
            {
                throw te;
            }
            catch (Exception e)
            {

                if (BatchDatabaseOptions.AbortOnError)
                {
                    throw new Exception("Error occured processing record - abort on error: " + recordId + " : " + e.Message, e);
                }
                else
                {

                    if (PrevErrMsg == e.Message)
                    {
                        RepeatedErrorCount++;
                    }
                    else
                    {
                        RepeatedErrorCount = 0;
                    }

                    if (RepeatedErrorCount > BatchDatabaseOptions.MaxRepeatedErrorCountBeforeAbort)
                    {
                        throw new Exception("Error occured processing record - too many repeated errors: " + recordId.ToString() + " : " + e.Message, e);
                    }
                    else
                    {
                        ErrorCount++;

                        if (ErrorCount > BatchDatabaseOptions.MaxErrorCountBeforeAbort)
                        {
                            throw new Exception("Error occured processing record  - too many errors: " + recordId.ToString() + " : " + e.Message, e);
                        }
                    }

                    PrevErrMsg = e.Message;
                }
            }
            return ret;
        }
        public override object ProcessRecord(object recordId, object record)
        {

            throw new Exception("Instance of  class CensusIntersectorDatabaseRunnerTest called, should be calling the instance of CensusIntersectorDatabaseRunner ");
        }
        public override bool UpdateRecord(object recordId, object result)
        {
            throw new Exception("Instance of  class CensusIntersectorDatabaseRunnerTest called, should be calling the instance of CensusIntersectorDatabaseRunner ");
        }
        public bool IsIndexSelected(string Index, String[] collection)
        {
            bool returnValue = false;
            for (int i = 0; i < collection.Length; i++)
            {
                if (collection[i] != null)
                {
                    if (collection[i].Equals((String)Index))
                    {
                        returnValue = true;
                        break;
                    }
                }

            }
            return returnValue;
        }

    }
}
