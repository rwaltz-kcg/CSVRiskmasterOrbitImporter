using System;
using System.IO;
using System.Configuration;
using System.Collections.Generic;
using System.Text;

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
    public class RiskMasterCsvParser
        {
        private static string _pf_import_id;
        private static string _pd_import_id;
        private static string _import_date;
        private static string[] _pfCsvColumnNames = new string[] {
            "PAYMENT_ID",
            "CONTROL_NUMBER",
            "CHECK_NUMBER",
            "PAYEE_NAME",
            "STREET_ADDRESS_1",
            "STREET_ADDRESS_2",
            "CITY",
            "STATE",
            "ZIP_CODE",
            "CHECK_MEMO",
            "CLAIM_NUMBER",
            "CLAIMANT_NAME",
            "CHECK_AMOUNT",
            "CHECK_DATE",
            "TRANSACTION_DATE",
            "STATUS_CODE",
            "PAYMENT_FLAG",
            "CLEARED_FLAG",
            "CLAIM_ID",
            "INVOICE_DATE",
            "CLAIMANT_FIRST_NAME",
            "CLAIMANT_LAST_NAME",
            "CLAIMANT_ABBREVIATION",
            "NPI_NUMBER",
            "TITLE" };

        private static string[] _pdCsvColumnNames = new string[] {
            "PAYMENT_ID",
            "PAYMENT_DETAIL_ID",
            "TRANSACTION_TYPE",
            "SPLIT_AMOUNT",
            "GENERAL_LEDGER_ACCOUNT",
            "FROM_DATE",
            "TO_DATE",
            "INVOICED_BY",
            "INVOICE_AMOUNT",
            "INVOICE_NUMBER",
            "PO_NUMBER"};

        private char _csvDelimiter;
        private char _csvQualifier;

        public RiskMasterCsvParser(char csvDelimiter, char csvQualifier)
            {
            _csvDelimiter = csvDelimiter;
            _csvQualifier = csvQualifier;
            RiskMasterCsvParser._import_date = DateTime.Today.ToString("yyyyMMdd");

            }
        /**
		 * prepare a datastructure that can be used to insert rows into the xxcok_rm_pd_import database table
		 */
        public IList<XVar> getPfCsvContents(string pfCsvFile)
            {
            IList<XVar> pfCsvContentsArrayList = new List<XVar>();
            // open the pf csv file,  parse all the contents and produce data structure
            RiskMasterCsvParser._pf_import_id = getFormatIdFromFilePath(pfCsvFile);
            string fileContents = File.ReadAllText(pfCsvFile);
            IList<string> allCsvRows = this.parseFileIntoLines(fileContents);
            if (allCsvRows.Count > 0)
                {
                foreach (string csvRow in allCsvRows)
                    {
                    if (csvRow.Length > 1)
                        {
                        IList<String> csvRowList = this.parseCsvRowtoList(csvRow);

                        XVar csvRowDictionary = this.parsePfCsvRowListToDictionary(csvRowList);
                        pfCsvContentsArrayList.Add(csvRowDictionary);
                        }
                    }
                }
            else
                {
                throw new Exception("Pf CSV file is empty");
                }
            return pfCsvContentsArrayList;
            }
        /**
		 * prepare a datastructure that can be used to insert rows into the xxcok_rm_pd_import database table
		 */
        public IList<XVar> getPdCsvContents(string pdCsvFile)
            {
            IList<XVar> pdCsvContentsArrayList = new List<XVar>();
            // open the pf csv file,  parse all the contents and produce data structure
            RiskMasterCsvParser._pd_import_id = getFormatIdFromFilePath(pdCsvFile);
            string fileContents = File.ReadAllText(pdCsvFile);
            IList<string> allCsvRows = this.parseFileIntoLines(fileContents);
            if (allCsvRows.Count > 0)
                {
                foreach (string csvRow in allCsvRows)
                    {
                    if (csvRow.Length > 1)
                        {
                        IList<String> csvRowList = this.parseCsvRowtoList(csvRow);

                        XVar csvRowDictionary = this.parsePdCsvRowListToDictionary(csvRowList);
                        pdCsvContentsArrayList.Add(csvRowDictionary);
                        }
                    }
                }
            else
                {
                throw new Exception("Pd CSV file is empty");
                }
            return pdCsvContentsArrayList;
            }
        private XVar parsePfCsvRowListToDictionary(IList<string> csvRowList)
            {
            XVar pfCsvRowDictionary = new XVar();
            pfCsvRowDictionary.SetArrayItem("IMPORT_ID", RiskMasterCsvParser._pf_import_id);
            pfCsvRowDictionary.SetArrayItem("IMPORT_DATE", RiskMasterCsvParser._import_date);
            pfCsvRowDictionary.SetArrayItem("IS_VALID", "0");
            pfCsvRowDictionary.SetArrayItem("IS_PROCESSED", "0");
            // row delimiter passed in?
            if (csvRowList.Count != 25)
                {
                throw new Exception("Pf CSV file should have 25 columns. Unable to discover 25 columns");
                }

            for (int rowNum = 0; rowNum < 25; ++rowNum)
                {
                pfCsvRowDictionary.SetArrayItem(_pfCsvColumnNames[rowNum], csvRowList[rowNum]);

                }

            //pfCsvRowDictionary.Add
            return pfCsvRowDictionary;
            }

        private XVar parsePdCsvRowListToDictionary(IList<string> csvRowList)
            {
            XVar pdCsvRowDictionary = new XVar();
            pdCsvRowDictionary.SetArrayItem("IMPORT_ID", RiskMasterCsvParser._pd_import_id);
            pdCsvRowDictionary.SetArrayItem("IMPORT_DATE", RiskMasterCsvParser._import_date);
            pdCsvRowDictionary.SetArrayItem("IS_VALID", "0");
            pdCsvRowDictionary.SetArrayItem("IS_PROCESSED", "0");
            // row delimiter passed in?
            if (csvRowList.Count != 11)
                {
                throw new Exception("Pd CSV file should have 11 columns. Unable to discover 11 columns");
                }

            for (int rowNum = 0; rowNum < 11; ++rowNum)
                {
                pdCsvRowDictionary.SetArrayItem(_pdCsvColumnNames[rowNum], csvRowList[rowNum]);

                }

            //pfCsvRowDictionary.Add
            return pdCsvRowDictionary;
            }
        private IList<String> parseCsvRowtoList(string csvRow)
            {

            IList<string> listOfValues = new List<string>();
            char[] charTextArray = csvRow.ToCharArray();
            bool startOfValue = true;
            bool endOfValue = false;
            bool parseString = false;
            char lastChar = '\n';
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < charTextArray.Length; i++)
                {
                if (charTextArray[i].Equals(this._csvDelimiter))
                    {
                    if (parseString)
                        {
                        // the string is being parsed, the delimiter is considered part of the value
                        stringBuilder.Append(charTextArray[i]);
                        }
                    else if (startOfValue)
                        {
                        // this condition handles an empty value
                        endOfValue = true;
                        }
                    else if (stringBuilder.Length > 0)
                        {
                        // this condition represents a value that has ended, and a new one will begin
                        endOfValue = true;
                        startOfValue = true;
                        }
                    else
                        {
                        // this condition represents the start of a new value
                        startOfValue = true;
                        }
                    }
                else if (charTextArray[i].Equals(this._csvQualifier))
                    {
                    if (startOfValue)
                        {
                        startOfValue = false;
                        parseString = true;
                        }
                    else if (parseString)
                        {
                        parseString = false;
                        endOfValue = true;
                        }
                    else
                        {
                        parseString = true;
                        stringBuilder.Append(charTextArray[i]);
                        }
                    }
                else
                    {
                    startOfValue = false;
                    stringBuilder.Append(charTextArray[i]);
                    }

                lastChar = charTextArray[i];
                if (endOfValue)
                    {
                    string valueOfField = stringBuilder.ToString();
                    listOfValues.Add(valueOfField.Trim());
                    stringBuilder = new StringBuilder();
                    endOfValue = false;
                    parseString = false;
                    }

                }
            if (stringBuilder.Length > 0)
                {
                string valueOfField = stringBuilder.ToString();
                listOfValues.Add(valueOfField.Trim());
                }
            else if (lastChar == this._csvDelimiter)
                {
                listOfValues.Add("");
                }


            return listOfValues;
            }
        private IList<string> parseFileIntoLines(string fileContent)
            {
            IList<string> allCsvRows = new List<string>();
 
            char[] charTextArray = fileContent.ToCharArray();
            bool startOfValue = true;
            bool endOfValue = false;
            bool parseString = false;
            bool endOfRow = false;
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < charTextArray.Length; i++)
                {
                if (charTextArray[i].Equals(this._csvDelimiter))
                    {
                    // the delimiter represents a break between two values. 
                    stringBuilder.Append(charTextArray[i]);
                    
                    if (startOfValue)
                        {
                        // this condition handles an empty value
                        endOfValue = true;
                        }
                    else if (stringBuilder.Length > 0)
                        {
                        // this condition represents a value that has ended, and a new one will begin
                        endOfValue = true;
                        startOfValue = true;
                        }
                    }
                else if (charTextArray[i].Equals(this._csvQualifier))
                    {
                    // the qualifier represents a value that is a string. numbers do not need qualifiers inside a value
                    stringBuilder.Append(charTextArray[i]);
                    if (startOfValue)
                        {
                        startOfValue = false;
                        parseString = true;
                        }
                    else if (parseString)
                        {
                        parseString = false;
                        endOfValue = true;
                        }
                    else
                        {
                        parseString = true;
                        }
                    }
                else if (charTextArray[i].Equals('\n') || charTextArray[i].Equals('\r'))
                    {
                    if (parseString)
                        {
                        stringBuilder.Append(charTextArray[i]);
                        }
                    else if (endOfValue)
                        {
                        // will trigger pushing the row onto the lines list
                        endOfRow = true;
                        } 
                    }
                else
                    {
                    startOfValue = false;
                    stringBuilder.Append(charTextArray[i]);
                    }

                if (endOfRow && (stringBuilder.Length > 0))
                    {
                    
                    string valueOfField = stringBuilder.ToString();

                    allCsvRows.Add(valueOfField.Trim());
                    stringBuilder = new StringBuilder();
                    endOfValue = false;
                    parseString = false;
                    endOfRow = false;
                    }

                }

            return allCsvRows;
            }
        private string getFormatIdFromFilePath(string filePath)
            {
            string fileName = Path.GetFileName(filePath);
            int fileExtPosition = fileName.LastIndexOf('.');
            string fileNameNoFileExt = fileName.Remove(fileExtPosition, (fileName.Length - fileExtPosition));
            string importID = fileNameNoFileExt.Substring(2);
            return importID;
            }
        }
    }
