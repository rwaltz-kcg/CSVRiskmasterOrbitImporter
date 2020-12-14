using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;

/**
    Risk Master Importer transfers data from RiskMaster CSV spreadsheets into Database Tables
    Copyright(C) 2020  City of Knoxville

    This program is free software: you can redistribute it and/or modify

	it under the terms of the GNU General Public License as published by

	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of

	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the

	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with this program.If not, see<https://www.gnu.org/licenses/>.
**/

namespace runnerDotNet
    {
    /// <summary>
    //  Searches through a directory to discover PD and PF files that have been exported by RiskMaster.
    //  For each PD and PF file found, the CSV files will be parsed into a Collection datastructure
    //  The parsed CSV datastructure will be uploaded to the ORBIT Import Routines tables
    // RISKMASTER_PF_IMPORT 
    // RISKMASTER_PD_IMPORT
    /// </summary>
    // 
    public class RiskMasterImporter
        {

        private RiskMasterCvsDbLoader _riskMasterCvsDbLoader;

        private RiskMasterCsvParser _riskMasterCsvParser;

        private string _processDirectory;

        public string ProcessDirectory
            {
            get { return _processDirectory; }
            set { _processDirectory = value; }
            }

        private string _archiveDirectory;

        public string ArchiveDirectory
            {
            get { return _archiveDirectory; }
            set { _archiveDirectory = value; }
            }

        private string _failDirectory;

        public string FailDirectory
            {
            get { return _failDirectory; }
            set { _failDirectory = value; }
            }

        private IList<String> _processedPfFiles;

        public IList<String> ProcessedPfFiles
            {
            get { return _processedPfFiles; }
            set { _processedPfFiles = value; }
            }

        private IList<String> _processedPdFiles;

        public IList<String> ProcessedPdFiles
            {
            get { return _processedPdFiles; }
            set { _processedPdFiles = value; }
            }

        private IList<String> _failedPfFiles;

        public IList<String> FailedPfFiles
            {
            get { return _failedPfFiles; }
            set { _failedPfFiles = value; }
            }

        private IList<String> _failedPdFiles;

        public IList<String> FailedPdFiles
            {
            get { return _failedPdFiles; }
            set { _failedPdFiles = value; }
            }

        public RiskMasterImporter(string processDirectory, string archiveDirectory, string failDirectory, char csvDelimiter, char csvQualifier)
            {
            this.ProcessDirectory = processDirectory;
            this.ArchiveDirectory = archiveDirectory;
            this.FailDirectory = failDirectory;
            this.ProcessedPfFiles = new List<string>();
            this.ProcessedPdFiles = new List<string>();
            this.FailedPfFiles = new List<string>();
            this.FailedPdFiles = new List<string>();
            this._riskMasterCsvParser = new RiskMasterCsvParser(csvDelimiter, csvQualifier);
            this._riskMasterCvsDbLoader = new RiskMasterCvsDbLoader();
            }

        public bool iterateParseAndInsertCvsFiles()
            {
            bool success = true;
            IEnumerable<string> pbCsvFiles = from file in Directory.EnumerateFiles(this.ProcessDirectory, "*.csv")
                                             where Path.GetFileName(file).ToLower().StartsWith("pb")
                                             select file;
            // Make certain the case of the letters in the file names match. make them all lower case
            IList<string> pbCvsFileList = renameFilesToLowerCase(pbCsvFiles);

            deletePbCvsFiles(pbCvsFileList);

            IEnumerable<string> pfCsvFiles = from file in Directory.EnumerateFiles(this.ProcessDirectory, "*.csv")
                                             where Path.GetFileName(file).ToLower().StartsWith("pf")
                                             select file;
            // Make certain the case of the letters in the file names match. make them all lower case
            IList<string> pfCvsFileList = renameFilesToLowerCase(pfCsvFiles);

            // iterate through the files, on each file call a csv parser and then insert them into the database table RISKMASTER_PF_IMPORT 
            // gather any PF successes and display as succeeded
            var pdCsvFiles = from file in Directory.EnumerateFiles(this.ProcessDirectory, "*.csv")
                             where Path.GetFileName(file).ToLower().StartsWith("pd")
                             select file;
            IList<string> pdCvsFileList = renameFilesToLowerCase(pdCsvFiles);

            foreach (string pfCvsFile in pfCvsFileList)
                {

                // iterate through the files, on each file call a csv parser and then insert them into the database table RISKMASTER_PF_IMPORT 

                string pfCvsFileName = Path.GetFileName(pfCvsFile);
                string csvDirectoryPath = Path.GetDirectoryName(pfCvsFile);
                string pdCvsFileName = pfCvsFileName.Replace("pf", "pd");
                string pdCsvFile = csvDirectoryPath + Path.DirectorySeparatorChar + pdCvsFileName;
                try
                    {
                    IList<XVar> pfCsvContentList;
                    if (pdCvsFileList.Contains(pdCsvFile) && File.Exists(pdCsvFile) && File.Exists(pfCvsFile))
                        {

                        pfCsvContentList = this._riskMasterCsvParser.getPfCsvContents(pfCvsFile);

                        try
                            {
                            this._riskMasterCvsDbLoader.loadPfCsv(pfCsvContentList);

                            IList<XVar> pdCsvContentList;
                            pdCsvContentList = this._riskMasterCsvParser.getPdCsvContents(pdCsvFile);
                            try
                                {
                                this._riskMasterCvsDbLoader.loadPdCsv(pdCsvContentList);

                                string pfArchiveFile = this.ArchiveDirectory + Path.DirectorySeparatorChar + pfCvsFileName;
                                File.Delete(pfArchiveFile);
                                File.Move(pfCvsFile, pfArchiveFile);
                                ProcessedPfFiles.Add(pfCvsFileName);

                                string pdArchiveFile = this.ArchiveDirectory + Path.DirectorySeparatorChar + pdCvsFileName;
                                File.Delete(pdArchiveFile);
                                File.Move(pdCsvFile, pdArchiveFile);
                                ProcessedPdFiles.Add(pdCvsFileName);

                                }
                            catch (Exception ex)
                                {
                                //remove all records that were added
                                this._riskMasterCvsDbLoader.removePdCsv(pdCsvContentList);
                                throw ex;
                                }
                            }
                        catch (Exception ex)
                            {
                            //remove all records that were added
                            this._riskMasterCvsDbLoader.removePfCsv(pfCsvContentList);
                            throw ex;
                            }
                        }
                    else
                        {
                        string pfFailFile = this.FailDirectory + Path.DirectorySeparatorChar + pfCvsFileName;
                        if (File.Exists(pfCvsFile))
                            {
                            File.Delete(pfFailFile);
                            File.Move(pfCvsFile, pfFailFile);
                            }
                        FailedPfFiles.Add(pfCvsFileName);


                        string pdFailFile = this.FailDirectory + Path.DirectorySeparatorChar + pdCvsFileName;
                        if (File.Exists(pdCsvFile))
                            {
                            File.Delete(pdFailFile);
                            File.Move(pdCsvFile, pdFailFile);
                            }
                        FailedPdFiles.Add(pdCvsFileName);

                        success = false;
                        }
                    }
                catch (Exception ex)
                    {
                    string pfFailFile = this.FailDirectory + Path.DirectorySeparatorChar + pfCvsFileName;
                    if (File.Exists(pfCvsFile))
                        {
                        File.Delete(pfFailFile);
                        File.Move(pfCvsFile, pfFailFile);
                        }
                    FailedPfFiles.Add(pfCvsFileName);

                    string pdFailFile = this.FailDirectory + Path.DirectorySeparatorChar + pdCvsFileName;
                    if (File.Exists(pdCsvFile))
                        {
                        File.Delete(pdFailFile);
                        File.Move(pdCsvFile, pdFailFile);
                        }
                    FailedPdFiles.Add(pdCvsFileName);
                    /**
                    Console.WriteLine("An exception ({0}) occurred.",ex.GetType().Name);
                    Console.WriteLine("   Message:\n{0}", ex.Message);
                    Console.WriteLine("   Stack Trace:\n   {0}", ex.StackTrace);
                    Exception ie = ex.InnerException;
                    if (ie != null)
                        {
                        Console.WriteLine("   The Inner Exception:");
                        Console.WriteLine("      Exception Name: {0}", ie.GetType().Name);
                        Console.WriteLine("      Message: {0}\n", ie.Message);
                        Console.WriteLine("      Stack Trace:\n   {0}\n", ie.StackTrace);
                        }
                    **/
                    success = false;
                    }
                }
            return success;
            }

        public IList<string> renameFilesToLowerCase(IEnumerable<string> csvFiles)
            {
            IList<string> renamedFilesList = new List<string>();
            foreach (string csvFile in csvFiles)
                {
                string csvFileName = Path.GetFileName(csvFile);
                string csvDirectoryPath = Path.GetDirectoryName(csvFile);
                if (!csvFileName.Equals(csvFileName.ToLower()))
                    {
                    string newFilePath = csvDirectoryPath + Path.DirectorySeparatorChar + csvFileName.ToLower();
                    File.Move(csvFile, newFilePath);
                    renamedFilesList.Add(newFilePath);
                    }
                else
                    {
                    renamedFilesList.Add(csvFile);
                    }
                }
            return renamedFilesList;
            }
        public void deletePbCvsFiles(IEnumerable<string> csvFiles)
            {

            foreach (string csvFile in csvFiles)
                {

                if (File.Exists(csvFile))
                    {
                    File.Delete(csvFile);
                    }
                }
            }
        }
    }
