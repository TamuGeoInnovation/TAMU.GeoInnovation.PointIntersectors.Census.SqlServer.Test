using System.Data;
using TAMU.GeoInnovation.PointIntersectors.Census.OutputData.CensusRecords;

namespace TAMU.GeoInnovation.PointIntersectors.Census.PointIntersecters.Interfaces
{
    public interface ISqlServerCensusPointIntersectorTest: ICensusPointIntersector
    {

        #region Properties


        #endregion


        CensusRecord GetRecord(double longitude, double latitude, string state, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected);
        CensusRecord GetRecord(double longitude, double latitude, string state, double version, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected);

        DataTable GetRecordAsDataTable(double longitude, double latitude, string state, string countyFips, double version, string Level1, string Level2, string Level3, string Level4, bool IsGeomterySelected);

        string GetPlaceFips(double longitude, double latitude, string state, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax, bool IsGeomterySelected);

        string GetSpatialIndex(bool SpatialIndex, string Level1, string Level2, string Level3, string Level4, string tableName, double xmin, double ymin, double xmax, double ymax, string CellsPerObject);
        void GenerateIndices(bool IndexType, string TableName, double xmin, double ymin, double xmax, double ymax, string CellsPerObject);
        void DeleteIndexes(string IndexName);
        void GenerateSingleIndexes(string TableName, string Level1, string Level2, string Level3, string Level4, double xmin, double ymin, double xmax, double ymax);
        DataTable GetBoundaryPoints(string TableName);

    }
}
