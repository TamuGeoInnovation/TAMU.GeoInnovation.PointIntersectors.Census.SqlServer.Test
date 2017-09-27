using System;

namespace USC.GISResearchLab.Census.Runners.Queries.Options
{
    [Serializable]
    public class BatchIntersectTest : BatchIntersect
    {
        #region Properties

        public bool IsInitialRun { get; set; }
        public bool AllIndices { get; set; }
        public bool Testing { get; set; }
        public bool GeographyIndexSelected { get; set; }
        public bool GeometryIndexSelected { get; set; }
        public String[] SelectedIndices = new String[81];
        public string CellsPerObject { get; set; }
        #endregion

    }
}
