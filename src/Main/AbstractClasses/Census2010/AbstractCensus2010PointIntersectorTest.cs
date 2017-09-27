using System;
using System.Collections.Generic;
using System.Data;
using TAMU.GeoInnovation.PointIntersectors.Census.OutputData.CensusRecords;
using USC.GISResearchLab.Census.Core.Configurations.ServerConfigurations;
using USC.GISResearchLab.Common.Databases.QueryManagers;


namespace TAMU.GeoInnovation.PointIntersectors.Census.Census2010
{
    [Serializable]
    public abstract class AbstractCensus2010PointIntersectorTest : AbstractCensus2010PointIntersector
    {

        #region Properties



        #endregion

        public AbstractCensus2010PointIntersectorTest()
            : base()
        {
            
        }

        public AbstractCensus2010PointIntersectorTest(double version, IQueryManager blockFilesQueryManager, IQueryManager stateFilesQueryManager, IQueryManager countryFilesQueryManager)
            : base(version, blockFilesQueryManager, stateFilesQueryManager, countryFilesQueryManager)
        {
            CensusYear = CensusYear.TwoThousandTen;
        }

        public AbstractCensus2010PointIntersectorTest(Version version, IQueryManager blockFilesQueryManager, IQueryManager stateFilesQueryManager, IQueryManager countryFilesQueryManager)
            : base(version, blockFilesQueryManager, stateFilesQueryManager, countryFilesQueryManager)
        {
            CensusYear = CensusYear.TwoThousandTen;
        }


        public List<string> Grid_Densities = new List<string>(new string[] { "LOW", "MEDIUM", "HIGH" });

        public int Cells_Per_Object = 400;

        public abstract CensusRecord GetRecord(double longitude, double latitude, string state, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected);

        public abstract CensusRecord GetRecord(double longitude, double latitude, string state, double version, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected);

        public abstract DataTable GetRecordAsDataTable(double longitude, double latitude, string state, string countyFips, double version, string Level1, string Level2, string Level3, string Level4, bool IsGeomterySelected);

        public abstract DataTable GetNearestBlockRecordAsDataTable(double longitude, double latitude, string state, double distanceThreshold, string Level1, string Level2, string Level3, string Level4, bool IsGeomterySelected);

        public abstract string GetCBSAFips(double longitude, double latitude, bool IsBatchProcess, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected);

        public abstract string GetMSAFips(string stateFips, string countyFips, string placeFips, bool IsBatchProcess, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax);

        public abstract string GetMetDivFips(double longitude, double latitude, bool IsBatchProcess, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected);

        public abstract string GetMCDFips(double longitude, double latitude, string state, string countyFips, bool IsBatchProcess, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected);

        public abstract string GetPlaceFips(double longitude, double latitude, string state, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected);

        public abstract string GetCountyFips(double longitude, double latitude, string state, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected);

        public abstract string GetSpatialIndex(bool GeometryIndexSelected, string Level1, string Level2, string Level3, string Level4, string tableName, double xmin, double ymin, double xmax, double ymax, string CellsPerObject);

        public abstract void GenerateIndices(bool IsGeometryIndexSelected, string TableName, double xmin, double ymin, double xmax, double ymax, string CellsPerObject);

        public abstract void DeleteIndexes(string TableName);

        public abstract DataTable GetBoundaryPoints(string TableName);

        public abstract void GenerateSingleIndexes(string TableName, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax);

    }
}
