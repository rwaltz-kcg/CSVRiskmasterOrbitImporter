using runnerDotNet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSVRiskmasterOrbitImporter
{
    class Program
    {
        static void Main(string[] args)
        {

			System.Text.StringBuilder outputLines = new System.Text.StringBuilder();
			string processDirectory = @"F:\RiskMasterImports";
			// string processDirectory = @"\\psoftweb\hrcommon\Risk\OrbitImporterRoutines\RiskMaster";
			string archiveDirectory = @"F:\RiskMasterImports\archive";
			//string archiveDirectory = @"\\psoftweb\hrcommon\Risk\OrbitImporterRoutines\RiskMaster\archive";
			string failDirectory = @"F:\RiskMasterImports\fail";
			//string failDirectory = @"\\psoftweb\hrcommon\Risk\OrbitImporterRoutines\RiskMaster\fail";

			char csvDelimiter = ',';
			char csvQualifier = '"';
            // RiskMasterImporter is a City of Knoxville custom built class created by Robert Waltz
            //RiskMasterImporter is found in  ASP_OrbitImportRoutines\source\include
            RiskMasterImporter riskMasterImporter = new RiskMasterImporter(processDirectory, archiveDirectory, failDirectory, csvDelimiter, csvQualifier);
			try
			{
				//PF files contain the parent relationship to PD files. Parse the PF files first

				bool success = riskMasterImporter.iterateParseAndInsertCvsFiles();
				if (riskMasterImporter.FailedPfFiles.Count > 0)
				{
					IList<String> failedPfFiles = riskMasterImporter.FailedPfFiles;
					foreach (string failedFile in failedPfFiles)
					{
						outputLines.Append(@"<font color='red'> PF file " + failedFile + @" failed</font> <br />");
					}
				}
				if (riskMasterImporter.ProcessedPfFiles.Count > 0)
				{
					IList<String> processedPfFiles = riskMasterImporter.ProcessedPfFiles;
					foreach (string processedFile in processedPfFiles)
					{
						outputLines.Append(@"<font color='blue'> PF file " + processedFile + @" completed</font> <br />");
					}
				}
				if (riskMasterImporter.FailedPdFiles.Count > 0)
				{
					IList<String> failedPdFiles = riskMasterImporter.FailedPdFiles;
					foreach (string failedFile in failedPdFiles)
					{
						outputLines.Append(@"<font color='red'> PD file " + failedFile + @" failed</font> <br />");
					}
				}
				if (riskMasterImporter.ProcessedPdFiles.Count > 0)
				{
					IList<String> processedPdFiles = riskMasterImporter.ProcessedPdFiles;
					foreach (string processedFile in processedPdFiles)
					{
						outputLines.Append(@"<font color='blue'> PD file " + processedFile + @" completed</font> <br />");
					}
				}
			}
			catch (System.Exception e)
			{
				outputLines.AppendFormat("<font color='red'>An exception ({0}) occurred.</font> <br />", e.GetType().Name);
				outputLines.AppendFormat("<font color='red'>&nbsp;&nbsp; Message:</font> <br /><font color=\'red\'>&nbsp;&nbsp; {0}</font> <br />", e.Message);
				outputLines.AppendFormat("<font color='red'>&nbsp;&nbsp; Stack Trace:</font> <br /><font color=\'red\'>&nbsp;&nbsp; {0}</font> <br />", e.StackTrace);
				System.Exception ie = e.InnerException;
				if (ie != null)
				{
					outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp The Inner Exception:</font> <br />");
					outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp&nbsp Exception Name: {0}</font> <br />", ie.GetType().Name);
					outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp Message: {0}</font> <br />", ie.Message);
					outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp Stack Trace:</font> <br /><font color=\'red\'>&nbsp;&nbsp;&nbsp {0}</font> <br />", ie.StackTrace);
				}
			}


			// Place event code here.
			// Use "Add Action" button to add code snippets.
			try
			{
				// RiskMasterPaymentsValidator is a City of Knoxville custom built class created by Robert Waltz
				// RiskMasterPaymentsValidator is found in  ASP_OrbitImportRoutines\source\include
				RiskMasterPaymentsValidator validator = new RiskMasterPaymentsValidator();
				validator.validate();
				if (validator.TotalRecordsValidated > 0)
				{
					outputLines.AppendFormat(@"<font color='blue'> {0} Record{1} Validated</font> <br />", validator.TotalRecordsValidated, validator.TotalRecordsValidated > 1 ? "s" : "");
				}
				else
				{
					outputLines.Append(@"<font color='blue'> No Records Validated</font> <br />");
				}
			}
			catch (System.Exception e)
			{
				outputLines.AppendFormat("<font color=\'red\'>An exception ({0}) occurred.</font> <br />", e.GetType().Name);
				outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp; Message:</font> <br /><font color=\'red\'>&nbsp;&nbsp; {0}</font> <br />", e.Message);
				outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp; Stack Trace:</font> <br /><font color=\'red\'>&nbsp;&nbsp; {0}</font> <br />", e.StackTrace);
				System.Exception ie = e.InnerException;
				if (ie != null)
				{
					outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp The Inner Exception:</font> <br />");
					outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp&nbsp Exception Name: {0}</font> <br />", ie.GetType().Name);
					outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp Message: {0}</font> <br />", ie.Message);
					outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp Stack Trace:</font> <br /><font color=\'red\'>&nbsp;&nbsp;&nbsp {0}</font> <br />", ie.StackTrace);
				}
			}
			Console.Out.WriteLine(outputLines.ToString());

		}
	}
}
