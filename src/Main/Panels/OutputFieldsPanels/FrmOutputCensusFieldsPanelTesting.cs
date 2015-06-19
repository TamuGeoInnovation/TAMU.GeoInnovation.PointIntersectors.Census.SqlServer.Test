using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using USC.GISResearchLab.Common.Databases.SchemaManagers;
using USC.GISResearchLab.Common.Databases.TypeConverters;
using USC.GISResearchLab.Common.Databases.DataTables;
using USC.GISResearchLab.Common.Utils.Databases.TableDefinitions;
using USC.GISResearchLab.Common.FieldMappings;
using USC.GISResearchLab.Common.Databases.FieldMappings;

namespace TAMU.GeoInnovation.PointIntersectors.Census.Panels.OutputFieldPanels
{
    public partial class FrmOutputCensusFieldsPanelTesting : Form
    {

        public ISchemaManager SchemaManager { get; set; }
        public string TableName { get; set; }
        public OutputDatabaseFieldMappings OutputFieldMappings { get; set; }
        public string Prefix { get; set; }
        public string FormName { get; set; }
        public bool IsDataBound { get; set; }

        public FrmOutputCensusFieldsPanelTesting(OutputDatabaseFieldMappings outputFieldMappings, string prefix)
        {
            OutputFieldMappings = outputFieldMappings;
            Prefix = prefix;
            InitializeComponent();

        }

        public void BindToConfiguration()
        {
            try
            {

                if (!IsDataBound)
                {

                    chkInclude.DataBindings.Add("Checked", OutputFieldMappings, "Enabled", true, DataSourceUpdateMode.OnPropertyChanged);

                    BindComboBox(cboCensusYear, OutputFieldMappings.GetFieldMapping("CensusYear"));
                    BindComboBox(cboStateFips, OutputFieldMappings.GetFieldMapping("StateFips"));
                    BindComboBox(cboNaaccrCensusTractCertaintyCode, OutputFieldMappings.GetFieldMapping("NaaccrCertCode"));
                    BindComboBox(cboNaaccrCensusTractCertaintyType, OutputFieldMappings.GetFieldMapping("NaaccrCertType"));
                    BindComboBox(cboCountyFips, OutputFieldMappings.GetFieldMapping("CountyFips"));
                    BindComboBox(cboCBSAFips, OutputFieldMappings.GetFieldMapping("CBSAFips"));
                    BindComboBox(cboCBSAMicro, OutputFieldMappings.GetFieldMapping("CBSAMicro"));
                    BindComboBox(cboMCDFips, OutputFieldMappings.GetFieldMapping("MCDFips"));
                    BindComboBox(cboMetDivFips, OutputFieldMappings.GetFieldMapping("MetDivFips"));
                    BindComboBox(cboMSAFips, OutputFieldMappings.GetFieldMapping("MSAFips"));
                    BindComboBox(cboPlaceFips, OutputFieldMappings.GetFieldMapping("PlaceFips"));
                    BindComboBox(cboTract, OutputFieldMappings.GetFieldMapping("Tract"));
                    BindComboBox(cboBlockGroup, OutputFieldMappings.GetFieldMapping("BlockGroup"));
                    BindComboBox(cboBlock, OutputFieldMappings.GetFieldMapping("Block"));

                    IsDataBound = true;
                }
            }
            catch (Exception ex)
            {
                string message = "Error binding to configuration:";
                message += Environment.NewLine;
                message += ex.Message;

                MessageBox.Show(this, message, "Exception Occurred", MessageBoxButtons.RetryCancel, MessageBoxIcon.Error, MessageBoxDefaultButton.Button2);
            }
        }


        public void SetAllComboBoxItems()
        {
            string[] columns = SchemaManager.GetColumnNames(TableName);
            foreach (Control control in Controls)
            {
                if (control.GetType() == typeof(ComboBox))
                {
                    SetComboBoxItems((ComboBox)control, columns);
                }
            }
        }

        public void SetComboBoxItems(ComboBox cbo, string [] items)
        {
            cbo.Items.Clear();
            cbo.Items.AddRange(items);
        }


        public void BindComboBox(ComboBox cbo, object dataSource)
        {
            cbo.DataBindings.Add("Text", dataSource, "Value", true, DataSourceUpdateMode.OnPropertyChanged);
            SetSelectedComboBoxItem(cbo, txtPrefix.Text +Prefix + ((FieldMapping)dataSource).DefaultValue);
        }

        public void SetSelectedComboBoxItem(ComboBox cbo, string value)
        {
            foreach (string item in cbo.Items)
            {
                if (String.Compare(item, value, true) == 0)
                {
                    cbo.SelectedItem = item;
                    break;
                }
            }
        }

        private void btnGenerate_Click(object sender, EventArgs e)
        {
            try
            {
                List<string> columnNames = new List<string>();
                List<int> columnMaxLengths = new List<int>();
                List<int> columnPrecisions = new List<int>();
                List<DatabaseSuperDataType> dataTypes = new List<DatabaseSuperDataType>();

                string prefix = txtPrefix.Text + Prefix;

                foreach (DatabaseFieldMapping fieldMapping in OutputFieldMappings.FieldMappings)
                {

                    columnNames.Add(prefix + fieldMapping.Name);
                    columnMaxLengths.Add(fieldMapping.MaxLength);
                    columnPrecisions.Add(fieldMapping.Precision);
                    dataTypes.Add(fieldMapping.Type);
                }

                if (chkInclude.Checked)
                {
                    SchemaManager.AddColumnsToTable(TableName, columnNames, dataTypes, columnMaxLengths, columnPrecisions, true);
                    SetAllComboBoxItems();

                    SetSelectedComboBoxItem(cboCensusYear, prefix + "CensusYear");
                    SetSelectedComboBoxItem(cboNaaccrCensusTractCertaintyCode, prefix + "NaaccrCertCode");
                    SetSelectedComboBoxItem(cboNaaccrCensusTractCertaintyType, prefix + "NaaccrCertType");
                    SetSelectedComboBoxItem(cboStateFips, prefix + "StateFips");
                    SetSelectedComboBoxItem(cboCountyFips, prefix + "CountyFips");
                    SetSelectedComboBoxItem(cboTract, prefix + "Tract");
                    SetSelectedComboBoxItem(cboBlockGroup, prefix + "BlockGroup");
                    SetSelectedComboBoxItem(cboBlock, prefix + "Block");
                    SetSelectedComboBoxItem(cboCBSAFips, prefix + "CBSAFips");
                    SetSelectedComboBoxItem(cboCBSAMicro, prefix + "CBSAMicro");
                    SetSelectedComboBoxItem(cboMCDFips, prefix + "MCDFips");
                    SetSelectedComboBoxItem(cboMetDivFips, prefix + "MetDivFips");
                    SetSelectedComboBoxItem(cboMSAFips, prefix + "MSAFips");
                    SetSelectedComboBoxItem(cboPlaceFips, prefix + "PlaceFips");
                }
            }
            catch (Exception ex)
            {
                string message = "Error creating fields:";
                message += Environment.NewLine;
                message += ex.Message;

                MessageBox.Show(this, message, "Exception Occurred", MessageBoxButtons.RetryCancel, MessageBoxIcon.Error, MessageBoxDefaultButton.Button2);
            }
        }

        private void FrmOutputAddressFieldsPanel_Load(object sender, EventArgs e)
        {
            SetAllComboBoxItems();
            BindToConfiguration();
            this.Text = FormName + " Output Fields";
        }

        private void FrmOutputAddressFieldsPanel_FormClosing(object sender, FormClosingEventArgs e)
        {
            if (e.CloseReason == CloseReason.UserClosing)
            {
                e.Cancel = true;
                Hide();
            }
        }

        private void btnAutoSelect_Click(object sender, EventArgs e)
        {
            SetSelectedColumns();
        }

        private void SetSelectedColumns()
        {
            try
            {
                if (chkInclude.Checked)
                {

                    string prefix = txtPrefix.Text + Prefix;

                    SetSelectedComboBoxItem(cboCensusYear, prefix + "CensusYear");
                    SetSelectedComboBoxItem(cboNaaccrCensusTractCertaintyCode, prefix + "NaaccrCertCode");
                    SetSelectedComboBoxItem(cboNaaccrCensusTractCertaintyType, prefix + "NaaccrCertType");
                    SetSelectedComboBoxItem(cboStateFips, prefix + "StateFips");
                    SetSelectedComboBoxItem(cboCountyFips, prefix + "CountyFips");
                    SetSelectedComboBoxItem(cboTract, prefix + "Tract");
                    SetSelectedComboBoxItem(cboBlockGroup, prefix + "BlockGroup");
                    SetSelectedComboBoxItem(cboBlock, prefix + "Block");
                    SetSelectedComboBoxItem(cboCBSAFips, prefix + "CBSAFips");
                    SetSelectedComboBoxItem(cboCBSAMicro, prefix + "CBSAMicro");
                    SetSelectedComboBoxItem(cboMCDFips, prefix + "MCDFips");
                    SetSelectedComboBoxItem(cboMetDivFips, prefix + "MetDivFips");
                    SetSelectedComboBoxItem(cboMSAFips, prefix + "MSAFips");
                    SetSelectedComboBoxItem(cboPlaceFips, prefix + "PlaceFips");

                }
            }
            catch (Exception ex)
            {
                string message = "Error setting fields:";
                message += Environment.NewLine;
                message += ex.Message;

                MessageBox.Show(this, message, "Exception Occurred", MessageBoxButtons.RetryCancel, MessageBoxIcon.Error, MessageBoxDefaultButton.Button2);
            }
        }
    }
}
