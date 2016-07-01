using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using USC.GISResearchLab;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Core.Databases;
using System.Data.SqlClient;
using USC.GISResearchLab.Common.Utils.Databases;
using System.Data;
using USC.GISResearchLab.Common.Utils.Strings;
using USC.GISResearchLab.AddressProcessing.Core.Standardizing;
using USC.GISResearchLab.AddressProcessing.Core.AddressNormalization.Implementations;
using TAMU.GeoInnovation.PointIntersectors.Census.PointIntersecters.AbstractClasses;
using USC.GISResearchLab.Census.Core.Configurations.ServerConfigurations;
using TAMU.GeoInnovation.PointIntersectors.Census.OutputData.CensusRecords;
using TAMU.GeoInnovation.PointIntersectors.Census.PointIntersectors.Implementations;
using USC.GISResearchLab.AddressProcessing.Core.Standardizing.StandardizedAddresses.Lines.LastLines;
using TAMU.GeoInnovation.PointIntersectors.Census.Census2010;


namespace TAMU.GeoInnovation.PointIntersectors.Census.SqlServer.Census2010
{
    [Serializable]
    public class SqlServerCensus2010PointIntersectorTest : AbstractCensus2010PointIntersectorTest
    {

        #region Properties



        #endregion

        public SqlServerCensus2010PointIntersectorTest()
            : base()
        { }

        public SqlServerCensus2010PointIntersectorTest(double version, IQueryManager blockFilesQueryManager, IQueryManager stateFilesQueryManager, IQueryManager countryFilesQueryManager)
            : base(version, blockFilesQueryManager, stateFilesQueryManager, countryFilesQueryManager)
        { }

        public SqlServerCensus2010PointIntersectorTest(Version version, IQueryManager blockFilesQueryManager, IQueryManager stateFilesQueryManager, IQueryManager countryFilesQueryManager)
           : base(version, blockFilesQueryManager, stateFilesQueryManager, countryFilesQueryManager)
        { }


        public override CensusRecord GetRecord(double longitude, double latitude, string state, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected)
        {
            return GetRecord(longitude, latitude, state, Version, Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax, IsGeomterySelected);
        }
        public override CensusRecord GetRecord(double longitude, double latitude, string state, double version, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected)
        {

            /* Code to generate the Geometry  Index  */
            //GenerateSingleIndexes(state+"_tabblock10", Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax);
            bool IsBatchProcess = true;
            CensusRecord ret = new CensusRecord();


            ret.Version = Version;
            ret.InputLatitude = latitude;
            ret.InputLongitude = longitude;
            ret.InputState = state;
            ret.CensusYear = CensusYear.TwoThousandTen;

            DateTime start = DateTime.Now;

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {


                    DateTime startQuery;
                    DateTime endQuery;

                    startQuery = DateTime.Now;
                    string countyFips = GetCountyFips(longitude, latitude, state, Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax, IsGeomterySelected);
                    //string countyFips = "";
                    endQuery = DateTime.Now;
                    TimeSpan total = endQuery.Subtract(startQuery);
                    double countyMilliseconds = total.TotalMilliseconds;

                    startQuery = DateTime.Now;
                    DataTable dataTable = GetRecordAsDataTable(longitude, latitude, state, null, version, Level1, Level2, Level3, Level4, IsGeomterySelected);
                    endQuery = DateTime.Now;
                    total = endQuery.Subtract(startQuery);
                    double totalMilliseconds = total.TotalMilliseconds;

                    // if there was no value at the intesection, try to get the nearest
                    if (dataTable == null || dataTable.Rows.Count == 0)
                    {
                        startQuery = DateTime.Now;
                        dataTable = GetNearestBlockRecordAsDataTable(longitude, latitude, state, 1000, Level1, Level2, Level3, Level4, IsGeomterySelected);
                        endQuery = DateTime.Now;
                        total = endQuery.Subtract(startQuery);
                        totalMilliseconds += total.TotalMilliseconds;
                    }

                    if (dataTable != null && dataTable.Rows.Count > 0)
                    {
                        ret.Version = Version;

                        ret.StateFips = Convert.ToString(dataTable.Rows[0]["stateFp10"]);
                        ret.CountyFips = Convert.ToString(dataTable.Rows[0]["countyFp10"]);

                        string tract = Convert.ToString(dataTable.Rows[0]["tractCe10"]);
                        if (!String.IsNullOrEmpty(tract))
                        {
                            if (tract.Length == 6)
                            {
                                ret.Tract = StringUtils.InsertCharAtPositionFromEnd(tract, ".", 2);
                            }
                            else
                            {
                                ret.Tract = tract;
                            }
                        }

                        ret.BlockGroup = Convert.ToString(dataTable.Rows[0]["blockCe10"]);
                        ret.Block = Convert.ToString(dataTable.Rows[0]["GeoId10"]);

                        if (version > 1.1)
                        {

                            if (!String.IsNullOrEmpty(ret.BlockGroup))
                            {
                                string block = ret.BlockGroup;
                                ret.Block = block;
                                ret.BlockGroup = block[0].ToString();
                            }
                        }


                        ret.CountyFipsTimeTaken = TimeSpan.FromMilliseconds(countyMilliseconds);

                        ret.BlockTimeTaken = TimeSpan.FromMilliseconds(totalMilliseconds / 5.0);

                        //ret.StateFipsTimeTaken = TimeSpan.FromMilliseconds(totalMilliseconds / 5.0);
                        //ret.BlockGroupTimeTaken = TimeSpan.FromMilliseconds(totalMilliseconds / 5.0);
                        //ret.BlockTimeTaken = TimeSpan.FromMilliseconds(totalMilliseconds / 5.0);
                        //ret.TractTimeTaken = TimeSpan.FromMilliseconds(totalMilliseconds / 5.0);


                        startQuery = DateTime.Now;
                        ret.PlaceFips = GetPlaceFips(longitude, latitude, state, Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax, IsGeomterySelected);
                        endQuery = DateTime.Now;
                        ret.PlaceFipsTimeTaken = endQuery.Subtract(startQuery);

                        startQuery = DateTime.Now;
                        ret.McdFips = GetMCDFips(longitude, latitude, state, countyFips, IsBatchProcess, Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax, IsGeomterySelected);
                        endQuery = DateTime.Now;
                        ret.McdFipsTimeTaken = endQuery.Subtract(startQuery);

                        startQuery = DateTime.Now;
                        ret.MetDivFips = GetMetDivFips(longitude, latitude, IsBatchProcess, Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax, IsGeomterySelected);
                        endQuery = DateTime.Now;
                        ret.MetDivFipsTimeTaken = endQuery.Subtract(startQuery);

                        startQuery = DateTime.Now;
                        ret.MsaFips = GetMSAFips(ret.StateFips, ret.CountyFips, ret.PlaceFips, IsBatchProcess, Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax);
                        endQuery = DateTime.Now;
                        ret.MsaFipsTimeTaken = endQuery.Subtract(startQuery);

                        startQuery = DateTime.Now;
                        ret.CbsaFips = GetCBSAFips(longitude, latitude, IsBatchProcess, Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax, IsGeomterySelected);
                        endQuery = DateTime.Now;
                        ret.CbsaFipsTimeTaken = endQuery.Subtract(startQuery);

                        startQuery = DateTime.Now;
                        ret.CbsaMicro = GetCBSAMicroFips(ret.CbsaFips);
                        endQuery = DateTime.Now;
                        ret.CbsaMicroTimeTaken = endQuery.Subtract(startQuery);

                    }
                }
            }
            catch (Exception e)
            {
                ret.Exception = e;
                ret.ExceptionOccurred = true;
                ret.Error = e.Message;
            }

            DateTime end = DateTime.Now;

            ret.TimeTaken = end.Subtract(start).TotalMilliseconds;

            return ret;
        }

        public override DataTable GetRecordAsDataTable(double longitude, double latitude, string state, string countyFips, double version, string Level1, string Level2, string Level3, string Level4, bool IsGeomterySelected)
        {

            DataTable ret = null;

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    if (StateUtils.isState(state))
                    {
                        string sql = "";
                        //sql += " USE " + QueryManager.Connection.Database + ";" ;
                        sql += " SELECT ";
                        sql += "  stateFp10, ";
                        sql += "  countyFp10, ";
                        sql += "  tractCe10, ";
                        sql += "  blockCe10, ";
                        sql += "  GeoId10 ";
                        sql += " FROM ";
                        sql += "[" + state + "_tabblock10 ]";
                        if (IsGeomterySelected)
                        {
                            sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geom))";
                            sql += " WHERE ";

                        }
                        else
                        {
                            sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geog))";
                            sql += " WHERE ";
                        }


                        // first implementation
                        //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                        // second implementation - attempt to speed it up by checking intersect on the point not the database row
                        //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                        // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                        //sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                        // fourth implementation, filter by county first
                        if (!String.IsNullOrEmpty(countyFips))
                        {
                            sql += "  countyFp10=@countyFips";
                            sql += "  AND ";
                        }
                        if (IsGeomterySelected)
                        {
                            sql += "  geometry::Point(@latitude, @longitude, 4269).STIntersects(shapeGeom) = 1";
                        }
                        else
                        {
                            sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";
                        }

                        SqlCommand cmd = new SqlCommand(sql);
                        if (!String.IsNullOrEmpty(countyFips))
                        {
                            cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("countyFips", SqlDbType.VarChar, countyFips));
                        }

                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                        IQueryManager qm = BlockFilesQueryManager;
                        qm.AddParameters(cmd.Parameters);
                        ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetBlockRecord: " + e.Message, e);
            }

            return ret;
        }

        public override DataTable GetNearestBlockRecordAsDataTable(double longitude, double latitude, string state, double distanceThreshold, string Level1, string Level2, string Level3, string Level4, bool IsGeomterySelected)
        {
            DataTable ret = null;

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    if (StateUtils.isState(state))
                    {
                        string sql = "";
                        //sql += " USE " + QueryManager.Connection.Database + ";" ;
                        sql += " SELECT ";
                        sql += "  TOP 1 ";
                        sql += "  stateFp10, ";
                        sql += "  countyFp10, ";
                        sql += "  tractCe10, ";
                        sql += "  blockCe10, ";
                        sql += "  GeoId10, ";
                        if (IsGeomterySelected)
                        {
                            sql += "  geometry::Point(@latitude1, @longitude1, 4269).STDistance(shapeGeom) as dist ";
                        }
                        else
                        {
                            sql += "  Geography::Point(@latitude1, @longitude1, 4269).STDistance(shapeGeog) as dist ";
                        }
                        sql += " FROM ";
                        sql += "[" + state + "_tabblock10 ]";

                        if (IsGeomterySelected)
                        {
                            sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geom))";
                            sql += " WHERE ";
                            sql += "  geometry::Point(@latitude2, @longitude2, 4269).STDistance(shapeGeom) <= @distanceThreshold ";
                        }
                        else
                        {
                            sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geog))";
                            sql += " WHERE ";
                            sql += "  Geography::Point(@latitude2, @longitude2, 4269).STDistance(shapeGeog) <= @distanceThreshold ";
                        }



                        sql += "  ORDER BY ";
                        sql += "  dist ";

                        SqlCommand cmd = new SqlCommand(sql);
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude1", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude1", SqlDbType.Decimal, longitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude2", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude2", SqlDbType.Decimal, longitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("distanceThreshold", SqlDbType.Decimal, distanceThreshold));

                        IQueryManager qm = BlockFilesQueryManager;
                        qm.AddParameters(cmd.Parameters);
                        ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetNearestBlockRecordAsDataTable: " + e.Message, e);
            }

            return ret;
        }

        public override string GetCBSAFips(double longitude, double latitude, bool IsBatchProcess, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected)
        {
            /* add code to add indexes to table us_cbsa10 here*/
            //GenerateSingleIndexes("us_cbsa10", Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax);

            string ret = "";

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    string sql = "";
                    sql += " SELECT ";
                    sql += "  CBSAFP10 ";
                    sql += " FROM ";
                    sql += "us_cbsa10 ";
                    if (IsGeomterySelected)
                    {
                        sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geom))";
                        sql += " WHERE ";
                        sql += "  geometry::Point(@latitude, @longitude, 4269).STIntersects(shapeGeom) = 1";
                    }
                    else
                    {
                        sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geog))";
                        sql += " WHERE ";
                        // first implementation
                        //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                        // second implementation - attempt to speed it up by checking intersect on the point not the database row
                        //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                        // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                        sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";
                    }

                    SqlCommand cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                    IQueryManager qm = CountryFilesQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetMCDFips: " + e.Message, e);
            }

            return ret;
        }


        public override string GetMSAFips(string stateFips, string countyFips, string placeFips, bool IsBatchProcess, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax)
        {

            string ret = "";

            try
            {

                string fromPlace = GetMSAFipsFromPlaceFips(stateFips, placeFips);

                if (String.IsNullOrEmpty(fromPlace))
                {
                    string fromCounty = GetMSAFipsFromCountyFips(stateFips, countyFips);
                    if (!String.IsNullOrEmpty(fromCounty))
                    {
                        ret = fromCounty;
                    }
                }
                else
                {
                    ret = fromPlace;
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetMSAFips: " + e.Message, e);
            }

            return ret;
        }


        public override string GetMetDivFips(double longitude, double latitude, bool IsBatchProcess, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected)
        {
            string ret = "";
            /*write code to create new indices on the database us_metDiv10*/
            //GenerateSingleIndexes("us_metDiv10", Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax);

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    string sql = "";
                    sql += " SELECT ";
                    sql += "  METDIVFP10 ";
                    sql += " FROM ";
                    sql += "us_metDiv10 ";
                    if (IsGeomterySelected)
                    {
                        sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geom))";
                        sql += " WHERE ";
                        sql += "  geometry::Point(@latitude, @longitude, 4269).STIntersects(shapeGeom) = 1";
                    }
                    else
                    {
                        sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geog))";
                        sql += " WHERE ";
                        // first implementation
                        //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                        // second implementation - attempt to speed it up by checking intersect on the point not the database row
                        //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                        // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                        sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";
                    }


                    SqlCommand cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                    IQueryManager qm = CountryFilesQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetMetDivFips: " + e.Message, e);
            }

            return ret;
        }

        public override string GetMCDFips(double longitude, double latitude, string state, string countyFips, bool IsBatchProcess, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected)
        {
            string ret = "";
            /*write code to create new indices on the database state_Cousub10*/
            //GenerateSingleIndexes(state + "_Cousub10", Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax);

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    string sql = "";
                    sql += " SELECT ";
                    sql += "  cousubFp10 ";
                    sql += " FROM ";
                    sql += state + "_cousub10 ";
                    if (IsGeomterySelected)
                    {
                        sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geom))";

                    }
                    else
                    {
                        sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geog))";
                    }
                    sql += " WHERE ";




                    // first implementation
                    //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                    // second implementation - attempt to speed it up by checking intersect on the point not the database row
                    //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                    // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                    //sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                    // fourth implementation - trim by in the right county first
                    if (!String.IsNullOrEmpty(countyFips))
                    {
                        sql += "  countyFp10=@countyFips";
                        sql += "  AND ";
                    }
                    if (IsGeomterySelected)
                    {
                        sql += "  geometry::Point(@latitude, @longitude, 4269).STIntersects(shapeGeom) = 1";
                    }
                    else
                    {
                        sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";
                    }

                    SqlCommand cmd = new SqlCommand(sql);

                    if (!String.IsNullOrEmpty(countyFips))
                    {
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("countyFips", SqlDbType.VarChar, countyFips));
                    }

                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                    IQueryManager qm = StateFilesQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetMCDFips: " + e.Message, e);
            }

            return ret;
        }

        public override string GetPlaceFips(double longitude, double latitude, string state, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected)
        {
            /*write code to create new indices on the database state_Place10*/
            //GenerateSingleIndexes(state+"_Place10", Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax);

            string ret = "";

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    string sql = "";
                    sql += " SELECT ";
                    sql += "  placeFp10 ";
                    sql += " FROM ";
                    sql += state + "_place10 ";
                    if (IsGeomterySelected)
                    {
                        sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geom))";
                        sql += " WHERE ";
                        sql += "  geometry::Point(@latitude, @longitude, 4269).STIntersects(shapeGeom) = 1";
                    }
                    else
                    {
                        sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geog))";
                        sql += " WHERE ";
                        // first implementation
                        //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                        // second implementation - attempt to speed it up by checking intersect on the point not the database row
                        //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                        // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                        sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";
                    }


                    SqlCommand cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                    IQueryManager qm = StateFilesQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetPlaceFips: " + e.Message, e);
            }

            return ret;
        }

        public override string GetCountyFips(double longitude, double latitude, string state, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected)
        {
            /*write code to create new indices on the database us_county10*/
            //GenerateSingleIndexes("us_county10", Level1, Level2, Level3, Level4, xmin, ymin, xmax, ymax);
            string ret = "";

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    string sql = "";
                    sql += " SELECT ";
                    sql += "  countyFp10 ";
                    sql += " FROM ";
                    sql += " us_county10 ";
                    if (IsGeomterySelected)
                    {
                        sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geom))";
                        sql += " WHERE ";
                        sql += "  geometry::Point(@latitude, @longitude, 4269).STIntersects(shapeGeom) = 1";
                    }
                    else
                    {
                        sql += " WITH (INDEX (" + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geog))";
                        sql += " WHERE ";
                        // first implementation
                        //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                        // second implementation - attempt to speed it up by checking intersect on the point not the database row
                        //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                        // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                        sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";
                    }





                    SqlCommand cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                    //IQueryManager qm = QueryManager;
                    IQueryManager qm = CountryFilesQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetCountyFips: " + e.Message, e);
            }

            return ret;
        }


        public override string GetSpatialIndex(bool GeometryIndexSelected, string Level1, string Level2, string Level3, string Level4, string tableName, double xmin, double ymin, double xmax, double ymax, string CellsPerObject)
        {
            string spatialIndexCode = "";
            if (GeometryIndexSelected)
            {
                if (tableName.Contains("tabblock10"))
                {
                    spatialIndexCode = "CREATE SPATIAL INDEX " + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geom   ON  [Tiger2010CensusBlocks].[dbo]." + tableName + "(shapeGeom ) USING GEOMETRY_GRID WITH (    BOUNDING_BOX = ( xmin=" + xmin + ", ymin=" + ymin + ", xmax=" + xmax + ", ymax=" + ymax + " ), GRIDS = (" + Level1 + "," + Level2 + "," + Level3 + "," + Level4 + "),    CELLS_PER_OBJECT = " + CellsPerObject + ");";

                }
                else if (tableName == "us_county10" || tableName == "us_MetDiv10" || tableName == "us_cbsa10")
                {
                    spatialIndexCode = "CREATE SPATIAL INDEX " + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geom   ON  [Tiger2010CountryFiles].[dbo]." + tableName + "(shapeGeom )   USING GEOMETRY_GRID WITH (    BOUNDING_BOX = ( xmin=" + xmin + ", ymin=" + ymin + ", xmax=" + xmax + ", ymax=" + ymax + " ),  GRIDS = (" + Level1 + "," + Level2 + "," + Level3 + "," + Level4 + "),    CELLS_PER_OBJECT = " + CellsPerObject + ");";

                }
                else if (tableName.Contains("place10") || tableName.Contains("cousub10"))
                {
                    spatialIndexCode = "CREATE SPATIAL INDEX " + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geom   ON  [Tiger2010StateFiles].[dbo]." + tableName + "(shapeGeom )   USING GEOMETRY_GRID WITH (    BOUNDING_BOX = ( xmin=" + xmin + ", ymin=" + ymin + ", xmax=" + xmax + ", ymax=" + ymax + " ),  GRIDS = (" + Level1 + "," + Level2 + "," + Level3 + "," + Level4 + "),    CELLS_PER_OBJECT = " + CellsPerObject + ");";

                }
            }
            else
            {
                if (tableName.Contains("tabblock10"))
                {
                    spatialIndexCode = "CREATE SPATIAL INDEX " + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geog ON  [Tiger2010CensusBlocks].[dbo]." + tableName + "(shapeGeog) USING GEOGRAPHY_GRID WITH ( GRIDS = (" + Level1 + "," + Level2 + "," + Level3 + "," + Level4 + "),CELLS_PER_OBJECT = " + CellsPerObject + ");";
                }
                else if (tableName == "us_county10" || tableName == "us_MetDiv10" || tableName == "us_cbsa10")
                {
                    spatialIndexCode = "CREATE SPATIAL INDEX " + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geog   ON  [Tiger2010CountryFiles].[dbo]." + tableName + "(shapeGeog)   USING GEOGRAPHY_GRID WITH ( GRIDS = (" + Level1 + "," + Level2 + "," + Level3 + "," + Level4 + "),CELLS_PER_OBJECT =" + CellsPerObject + ");";

                }
                else if (tableName.Contains("place10") || tableName.Contains("cousub10"))
                {
                    spatialIndexCode = "CREATE SPATIAL INDEX " + Level1 + "_" + Level2 + "_" + Level3 + "_" + Level4 + "Geog   ON  [Tiger2010StateFiles].[dbo]." + tableName + "(shapeGeog)   USING GEOGRAPHY_GRID WITH ( GRIDS = (" + Level1 + "," + Level2 + "," + Level3 + "," + Level4 + "),CELLS_PER_OBJECT = " + CellsPerObject + ");";

                }
            }

            return spatialIndexCode;
        }

        public override void GenerateIndices(bool IsGeometryIndexSelected, string TableName, double xmin, double ymin, double xmax, double ymax, string CellsPerObject)
        {

            for (int i = 0; i < 3; i++)
            {
                for (int j = 0; j < 3; j++)
                {
                    for (int k = 0; k < 3; k++)
                    {
                        for (int l = 0; l < 3; l++)
                        {
                            SqlCommand cmd = new SqlCommand(GetSpatialIndex(IsGeometryIndexSelected, Grid_Densities[i], Grid_Densities[j], Grid_Densities[k], Grid_Densities[l], TableName, xmin, ymin, xmax, ymax, CellsPerObject));
                            IQueryManager qm = StateFilesQueryManager;
                            int rows = qm.ExecuteNonQuery(CommandType.Text, cmd.CommandText, 10000000, true);
                        }
                    }
                }
            }


        }

        public override void DeleteIndexes(string TableName)
        {

            for (int i = 0; i < 3; i++)
            {
                for (int j = 0; j < 3; j++)
                {
                    for (int k = 0; k < 3; k++)
                    {
                        for (int l = 0; k < 3; k++)
                        {
                            SqlCommand cmd = new SqlCommand("DROP INDEX " + Grid_Densities[i] + "_" + Grid_Densities[j] + "_" + Grid_Densities[k] + "_" + Grid_Densities[l] + "Geom ON " + "[Tiger2010CensusBlocks].[dbo]." + TableName + ";DROP INDEX " + Grid_Densities[i] + "_" + Grid_Densities[j] + "_" + Grid_Densities[k] + "_" + Grid_Densities[l] + "Geog ON [Tiger2010CensusBlocks].[dbo]." + TableName + ";");
                            IQueryManager qm = StateFilesQueryManager;
                            int rows = qm.ExecuteNonQuery(CommandType.Text, cmd.CommandText, 10000000, true);
                        }
                    }
                }
            }


        }

        public override DataTable GetBoundaryPoints(string TableName)
        {
            DataTable ret = null;
            try
            {
                string sqlcode = "SELECT  geometry::EnvelopeAggregate(shapeGeom).STPointN(1).STX AS MinX,  geometry::EnvelopeAggregate(shapeGeom).STPointN(1).STY AS MinY,  geometry::EnvelopeAggregate(shapeGeom).STPointN(3).STX AS MaxX,  geometry::EnvelopeAggregate(shapeGeom).STPointN(3).STY AS MaxY  from " + TableName + ";";
                SqlCommand cmd = new SqlCommand(sqlcode);
                IQueryManager qm = StateFilesQueryManager;
                ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, 100000, true);
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred in Getboundarypoints: " + e.Message, e);
            }
            return ret;
        }

        public override void GenerateSingleIndexes(string TableName, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax)
        {


            SqlCommand cmd = new SqlCommand(GetSpatialIndex(true, Level1, Level2, Level3, Level4, TableName, xmin, ymin, xmax, ymax, "null"));
            IQueryManager qm = StateFilesQueryManager;
            int rows = qm.ExecuteNonQuery(CommandType.Text, cmd.CommandText, 10000000, true);



        }

    }
}