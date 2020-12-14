using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
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
    public class OracleDBFacade
        {
        IDictionary<string, string> _pfCsvColumnDataTypesDictionary = new Dictionary<string, string>();
        IDictionary<string, string> _pdCsvColumnDataTypesDictionary = new Dictionary<string, string>();
        IDictionary<string, string> _xxcokRiskMasterPaymentsColumnDataTypesDictionary = new Dictionary<string, string>();
        IDictionary<string, string> _apSupplierSitesAllColumnDataTypesDictionary = new Dictionary<string, string>();
        IDictionary<string, string> _appsXxcokApSuppliersVwColumnDataTypesDictionary = new Dictionary<string, string>();
        IDictionary<string, string> _xxcokRiskmasterPaymentError = new Dictionary<string, string>();
        IDictionary<string, string> _pfPKCsvColumnDataTypesDictionary = new Dictionary<string, string>();
        IDictionary<string, string> _pdPKCsvColumnDataTypesDictionary = new Dictionary<string, string>();

        public OracleDBFacade()
            {
            _pfCsvColumnDataTypesDictionary.Add("IMPORT_ID", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("IMPORT_DATE", "DATE");
            _pfCsvColumnDataTypesDictionary.Add("PAYMENT_ID", "NUMBER");
            _pfCsvColumnDataTypesDictionary.Add("CONTROL_NUMBER", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("CHECK_NUMBER", "NUMBER");
            _pfCsvColumnDataTypesDictionary.Add("PAYEE_NAME", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("STREET_ADDRESS_1", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("STREET_ADDRESS_2", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("CITY", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("STATE", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("ZIP_CODE", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("CHECK_MEMO", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("CLAIM_NUMBER", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("CLAIMANT_NAME", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("CHECK_AMOUNT", "NUMBER");
            _pfCsvColumnDataTypesDictionary.Add("CHECK_DATE", "DATE");
            _pfCsvColumnDataTypesDictionary.Add("TRANSACTION_DATE", "DATE");
            _pfCsvColumnDataTypesDictionary.Add("STATUS_CODE", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("PAYMENT_FLAG", "NUMBER");
            _pfCsvColumnDataTypesDictionary.Add("CLEARED_FLAG", "NUMBER");
            _pfCsvColumnDataTypesDictionary.Add("CLAIM_ID", "NUMBER");
            _pfCsvColumnDataTypesDictionary.Add("INVOICE_DATE", "DATE");
            _pfCsvColumnDataTypesDictionary.Add("CLAIMANT_FIRST_NAME", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("CLAIMANT_LAST_NAME", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("SUBSTR(CLAIMANT_ABBREVIATION,0,10)", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("NPI_NUMBER", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("TITLE", "STRING");
            _pfCsvColumnDataTypesDictionary.Add("IS_VALID", "NUMBER");
            _pfCsvColumnDataTypesDictionary.Add("IS_PROCESSED", "NUMBER");

            _pfPKCsvColumnDataTypesDictionary.Add("IMPORT_ID", "STRING");
            _pfPKCsvColumnDataTypesDictionary.Add("IMPORT_DATE", "DATE");
            _pfPKCsvColumnDataTypesDictionary.Add("PAYMENT_ID", "NUMBER");

            _pdCsvColumnDataTypesDictionary.Add("IMPORT_ID", "STRING");
            _pdCsvColumnDataTypesDictionary.Add("IMPORT_DATE", "DATE");
            _pdCsvColumnDataTypesDictionary.Add("PAYMENT_ID", "NUMBER");
            _pdCsvColumnDataTypesDictionary.Add("PAYMENT_DETAIL_ID", "NUMBER");
            _pdCsvColumnDataTypesDictionary.Add("TRANSACTION_TYPE", "STRING");
            _pdCsvColumnDataTypesDictionary.Add("SPLIT_AMOUNT", "NUMBER");
            _pdCsvColumnDataTypesDictionary.Add("GENERAL_LEDGER_ACCOUNT", "STRING");
            _pdCsvColumnDataTypesDictionary.Add("FROM_DATE", "DATE");
            _pdCsvColumnDataTypesDictionary.Add("TO_DATE", "DATE");
            _pdCsvColumnDataTypesDictionary.Add("INVOICED_BY", "STRING");
            _pdCsvColumnDataTypesDictionary.Add("INVOICE_AMOUNT", "NUMBER");
            _pdCsvColumnDataTypesDictionary.Add("INVOICE_NUMBER", "STRING");
            _pdCsvColumnDataTypesDictionary.Add("PO_NUMBER", "STRING");

            _pdPKCsvColumnDataTypesDictionary.Add("IMPORT_ID", "STRING");
            _pdPKCsvColumnDataTypesDictionary.Add("IMPORT_DATE", "DATE");
            _pdPKCsvColumnDataTypesDictionary.Add("PAYMENT_ID", "NUMBER");
            _pdPKCsvColumnDataTypesDictionary.Add("PAYMENT_DETAIL_ID", "NUMBER");

            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("CTL_NUMERIC", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("CLAIM_ID", "NUMBER");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("CLAIM_NUMERIC", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("NAME", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("ADDR1", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("ADDR2", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("CITY", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("STATE", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("ZIP_CODE", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("AMOUNT", "NUMBER");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("TRANS_DATE", "DATE");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("INVOICE_NUMERIC", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("INVOICE_DATE", "DATE");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("TRANSACTION_TYPE_DESCR", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("DEPT", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("LINE_OF_BUS_CODE", "NUMBER");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("DEPT_ASSIGNED_EID", "NUMBER");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("DEPT_DESCR", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("SUPPLIER_NBR", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("VENDOR_SITE_CODE", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("INVOICE_TOTAL", "NUMBER");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("TRANS_NUMERIC", "NUMBER");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("PRIMARY_KEY", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("CREATED_BY", "STRING");

            _apSupplierSitesAllColumnDataTypesDictionary.Add("VENDOR_SITE_CODE", "STRING");
            _apSupplierSitesAllColumnDataTypesDictionary.Add("VENDOR_ID", "NUMBER");
            _apSupplierSitesAllColumnDataTypesDictionary.Add("PAY_SITE_FLAG", "STRING");

            _appsXxcokApSuppliersVwColumnDataTypesDictionary.Add("VENDOR_ID", "NUMBER");

            _xxcokRiskmasterPaymentError.Add("IMPORT_ID", "STRING");
            _xxcokRiskmasterPaymentError.Add("IMPORT_DATE", "DATE");
            _xxcokRiskmasterPaymentError.Add("PAYMENT_ID", "NUMBER");
            _xxcokRiskmasterPaymentError.Add("ERROR_ID", "NUMBER");
            _xxcokRiskmasterPaymentError.Add("ERROR_DESCR", "STRING");
            }

        public dynamic Select(dynamic _param_table, dynamic _param_userConditions = null)
            {
            string whereClause = "";
            StringBuilder selectStatement = new StringBuilder();
            if (_param_userConditions is string)
                {
                whereClause = _param_userConditions;
                }
            else
                {
                if (_param_table.Equals("xxcok.xxcok_rm_import_pf"))
                    {

                    whereClause = buildWhereClauseColumnNamesAndValues(this._pfCsvColumnDataTypesDictionary, (XVar)_param_userConditions);
                    }
                else if (_param_table.Equals("xxcok.xxcok_risk_master_payments"))
                    {
                    whereClause = buildWhereClauseColumnNamesAndValues(this._xxcokRiskMasterPaymentsColumnDataTypesDictionary, (XVar)_param_userConditions);
                    }
                else if (_param_table.Equals("ap.ap_supplier_sites_all"))
                    {
                    whereClause = buildWhereClauseColumnNamesAndValues(this._apSupplierSitesAllColumnDataTypesDictionary, (XVar)_param_userConditions);
                    }
                else if (_param_table.Equals("apps.xxcok_ap_suppliers_vw"))
                    {
                    whereClause = buildWhereClauseColumnNamesAndValues(this._appsXxcokApSuppliersVwColumnDataTypesDictionary, (XVar)_param_userConditions);
                    }
                }
            selectStatement.AppendFormat("SELECT * FROM {0} WHERE {1}", _param_table, whereClause);

            dynamic queryResult = DB.Query(selectStatement.ToString());
            return queryResult;
            }
        public dynamic Delete(dynamic _param_table, dynamic _param_userConditions = null)
            {
            XVar deleteResult = new XVar();
            string whereClause = "";
            StringBuilder deleteStatement = new StringBuilder();
            if (_param_userConditions is string)
                {
                whereClause = _param_userConditions;
                }
            else
                {
                if (_param_table.Equals("xxcok.xxcok_rm_import_pf"))
                    {

                    whereClause = buildWhereClauseColumnNamesAndValues(this._pfPKCsvColumnDataTypesDictionary, (XVar)_param_userConditions);
                    }
                else if (_param_table.Equals("xxcok.xxcok_rm_import_pd"))
                    {
                    whereClause = buildWhereClauseColumnNamesAndValues(this._pdPKCsvColumnDataTypesDictionary, (XVar)_param_userConditions);
                    }

                }
            deleteStatement.AppendFormat("DELETE FROM {0} WHERE {1}", _param_table, whereClause);

            DB.Exec(deleteStatement.ToString());
            return deleteResult;
        }
        public dynamic Exec(dynamic _param_sql)
            {
            dynamic queryResult = DB.Exec(_param_sql);
            return queryResult;
            }
        public dynamic Query(dynamic _param_sql)
            {
            dynamic queryResult = DB.Query(_param_sql);
            return queryResult;
            }
        public XVar Insert(string tableName, dynamic data)
            {
            XVar insertResult = new XVar();
            string[] columnAndValues = new string[0];
            StringBuilder insertStatement = new StringBuilder();
            if (tableName.Equals("xxcok.xxcok_rm_import_pf"))
                {
                columnAndValues = buildInsertColumnNamesAndValues(this._pfCsvColumnDataTypesDictionary, (XVar)data);

                }
            else if (tableName.Equals("xxcok.xxcok_rm_import_pd"))
                {
                columnAndValues = buildInsertColumnNamesAndValues(this._pdCsvColumnDataTypesDictionary, (XVar)data);
                }
            else if (tableName.Equals("xxcok.xxcok_rm_import_error"))
                {
                columnAndValues = buildInsertColumnNamesAndValues(this._xxcokRiskmasterPaymentError, (XVar)data);
                }
            if (columnAndValues.Length == 0)
                {
                throw new Exception("tablename is all wrong: " + tableName);
                }


            insertStatement.AppendFormat("INSERT INTO {0} ({1}) VALUES ({2})", tableName, columnAndValues[0], columnAndValues[1]);
            DB.Query(insertStatement.ToString());

            return insertResult;
            }
        public XVar Update(dynamic _param_table, dynamic _param_data, dynamic _param_userConditions)
            {
            XVar updateResult = new XVar();
            string[] columnAndValues = new string[0];
            string whereClause = "";
            StringBuilder updateStatement = new StringBuilder();
            if (_param_table.Equals("xxcok.xxcok_rm_import_pf"))
                {
                columnAndValues = buildUpdateColumnNamesAndValues(this._pfCsvColumnDataTypesDictionary, (XVar)_param_data);
                whereClause = buildWhereClauseColumnNamesAndValues(this._pfCsvColumnDataTypesDictionary, (XVar)_param_userConditions);
                }
            if (columnAndValues.Length == 0)
                {
                throw new Exception("tablename is all wrong: " + _param_table);
                }


            updateStatement.AppendFormat("UPDATE {0} SET {1} WHERE {2}", _param_table, columnAndValues[0], whereClause);

            DB.Query(updateStatement.ToString());

            return updateResult;
            }

        private string[] buildInsertColumnNamesAndValues(IDictionary<string, string> csvColumnDataTypesDictionary, XVar data)
            {
            string[] returnStrings = new string[2];
            StringBuilder csvColumns = new StringBuilder();
            StringBuilder csvValues = new StringBuilder();
            foreach (KeyValuePair<string, string> kvp in csvColumnDataTypesDictionary)
                {
                if (data.KeyExists(kvp.Key))
                    {
                    if (csvColumns.Length > 0)
                        {
                        csvColumns.Append(",");
                        csvValues.Append(",");
                        }
                    csvColumns.Append(kvp.Key);
                    if (data.GetArrayItem(kvp.Key).Length() == 0)
                        {
                        csvValues.Append("NULL");
                        }
                    else if (kvp.Value.Equals("STRING"))
                        {
                        string valueString = data.GetArrayItem(kvp.Key).Replace("'", "''");
                        csvValues.AppendFormat("{0}{1}{2}", "'", valueString, "'");
                        }
                    else if (kvp.Value.Equals("NUMBER"))
                        {
                        csvValues.Append(data.GetArrayItem(kvp.Key).ToString());
                        }
                    else if (kvp.Value.Equals("DATE"))
                        {
                        csvValues.AppendFormat("{0}{1}{2}", "TO_DATE('", data.GetArrayItem(kvp.Key).ToString(), "', 'YYYYMMDD')");
                        // csvValues.AppendFormat("{0}{1}{2}", "'", data[kvp.Key], "'");
                        }
                    else
                        {
                        throw new Exception("Could not file datatype of " + kvp.Value);
                        }
                    }
                }
            returnStrings[0] = csvColumns.ToString();
            returnStrings[1] = csvValues.ToString();
            return returnStrings;
            }
        private string[] buildUpdateColumnNamesAndValues(IDictionary<string, string> csvColumnDataTypesDictionary, XVar data)
            {
            string[] returnStrings = new string[1];
            StringBuilder updateStmtBuilder = new StringBuilder();

            foreach (KeyValuePair<string, string> kvp in csvColumnDataTypesDictionary)
                {
                if (data.KeyExists(kvp.Key))
                    {
                    if (data.GetArrayItem(kvp.Key).Length() == 0)
                        {
                        updateStmtBuilder.AppendFormat("{0} = NULL", kvp.Key);
                        }
                    else if (kvp.Value.Equals("STRING"))
                        {
                        string valueString = data.GetArrayItem(kvp.Key).Replace("'", "''");
                        updateStmtBuilder.AppendFormat("{0}={1}{2}{3}", kvp.Key, "'", valueString, "'");
                        }
                    else if (kvp.Value.Equals("NUMBER"))
                        {
                        updateStmtBuilder.AppendFormat("{0}={1}", kvp.Key, data.GetArrayItem(kvp.Key).ToString());
                        }
                    else if (kvp.Value.Equals("DATE"))
                        {
                        updateStmtBuilder.AppendFormat("{0} = {1}{2}{3}", kvp.Key, "TO_DATE('", data.GetArrayItem(kvp.Key).ToString(), "', 'YYYYMMDD')");
                        }
                    else
                        {
                        throw new Exception("Could not file datatype of " + kvp.Value);
                        }
                    }
                }
            returnStrings[0] = updateStmtBuilder.ToString();

            return returnStrings;
            }
        private string buildWhereClauseColumnNamesAndValues(IDictionary<string, string> csvColumnDataTypesDictionary, XVar data)
            {
            string returnString = "";
            StringBuilder whereStmtBuilder = new StringBuilder();

            foreach (KeyValuePair<string, string> kvp in csvColumnDataTypesDictionary)
                {
                if (data.KeyExists(kvp.Key))
                    {
                    if (whereStmtBuilder.Length > 0)
                        {
                        whereStmtBuilder.Append(" AND ");
                        }
                    if (kvp.Value.Equals("STRING"))
                        {
                        string valueString = data[kvp.Key].Replace("'", "''");
                        whereStmtBuilder.AppendFormat("{0} = {1}{2}{3}", kvp.Key, "'", valueString, "'");
                        }
                    else if (kvp.Value.Equals("NUMBER"))
                        {
                        whereStmtBuilder.AppendFormat("{0} = {1}", kvp.Key, data[kvp.Key]);
                        }
                    else if (kvp.Value.Equals("DATE"))
                        {
                        whereStmtBuilder.AppendFormat("{0} = {1}{2}{3}", kvp.Key, "TO_DATE('", data[kvp.Key], "', 'YYYYMMDD')");
                        }
                    else
                        {
                        throw new Exception("Could not file datatype of " + kvp.Value);
                        }
                    }
                }
            returnString = whereStmtBuilder.ToString();

            return returnString;
            }

        }
    }
