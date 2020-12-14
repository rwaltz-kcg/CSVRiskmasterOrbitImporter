using System;
using System.Collections.Generic;
using System.Text;

namespace runnerDotNet
    {
    /*
     *          
    Check for Validity
    a)	NPI_Number  is a Required Field in RISKMASTER_PF_IMPORT table. Error shows  "Supplier NBR is missing in Entity NPI_NUMBER".
    b)	 An NPI_Number  must exist in XXCOK_AP_SUPPLIERS_VW. Error shows "Supplier NBR is invalid in Entity NPI_NUMBER"
    c)	Title is a Required Field in RISKMASTER_PF_IMPORT table. Error shows "Site Codes is missing in Entity Title"
    d)	Pay Site record must exist on  AP_SUPPLIER_SITES_ALL for NPI_Number as Vendor_ID = NPI_Number and Pay_Site_Flag = ‘Y’ and VENDOR_SITE_CODE = upper(Title). Error shows "Supplier Site Code is invalid in Entity Title"

     */

    public class RiskMasterPaymentsValidator
        {

        OracleDBFacade oracleDBFacade = new OracleDBFacade();
        IList<XVar> insertErrorList = new List<XVar>();
        private static string findPfImportToProcessWhereClause = @" IS_PROCESSED = 0 
                                            AND NOT EXISTS 
                                            (SELECT 1 
                                            FROM xxcok.xxcok_rm_import_error 
                                            WHERE xxcok.xxcok_rm_import_pf.IMPORT_ID = xxcok.xxcok_rm_import_error.IMPORT_ID 
                                            AND xxcok.xxcok_rm_import_pf.IMPORT_DATE = xxcok.xxcok_rm_import_error.IMPORT_DATE 
                                            AND xxcok.xxcok_rm_import_pf.PAYMENT_ID = xxcok.xxcok_rm_import_error.PAYMENT_ID)";

        private int _totalRecordsValidated = 0;
        public int TotalRecordsValidated
            {
            get { return _totalRecordsValidated; }
            set { _totalRecordsValidated = value; }
            }
        //OracleDB ORACLEDB = new OracleDB();
        public RiskMasterPaymentsValidator()
            {
            }
        public void validate()
            {
            try
                {

                IList<XVar> invalidCokRiskmasterPfRows = new List<XVar>();
                IList<XVar> validCokRiskmasterPfRows = new List<XVar>();

                dynamic dynamicPfImportResult = oracleDBFacade.Select("xxcok.xxcok_rm_import_pf", findPfImportToProcessWhereClause);
                if (dynamicPfImportResult != null)
                    {
                    dynamic record = dynamicPfImportResult.fetchAssoc();

                    while (record != null && record.Count() > 0)
                        {
                        bool valid = this.validateCokRiskmasterPfImportRecord(record.Value);
                        if (valid)
                            {
                            XVar validRecordWhereClause = buildPfImportRecordPrimaryKeyClause(record.Value);
                            if (validRecordWhereClause != null)
                                {
                                validCokRiskmasterPfRows.Add(validRecordWhereClause);
                                }
                            }
                        else
                            {
                            XVar invalidRecordWhereClause = buildPfImportRecordPrimaryKeyClause(record.Value);
                            if (invalidRecordWhereClause != null)
                                {
                                invalidCokRiskmasterPfRows.Add(invalidRecordWhereClause);
                                }
                            }
                        record = dynamicPfImportResult.fetchAssoc();
                        }


                    foreach (XVar errorRow in insertErrorList)
                        {
                        oracleDBFacade.Insert("xxcok.xxcok_rm_import_error", errorRow);
                        }
                    foreach (XVar invalidRecordWhereClause in invalidCokRiskmasterPfRows)
                        {
                        this.updateCokRiskmasterPfImportRecord(invalidRecordWhereClause, "0");
                        }
                    foreach (XVar validRecordWhereClause in validCokRiskmasterPfRows)
                        {
                        this.updateCokRiskmasterPfImportRecord(validRecordWhereClause, "1");
                        }

                    if (validCokRiskmasterPfRows.Count > 0)
                        {
                        this.countValidRiskMasterPayments();
                        this.populateRiskMasterPaymentsTable();
                        this.updateRiskMasterPFImportTables();
                        }
                    }
                }
            catch (Exception validateException)
                {



                Console.WriteLine("An exception ({0}) occurred.",
                                  validateException.GetType().Name);
                Console.WriteLine("   Message:\n{0}", validateException.Message);
                Console.WriteLine("   Stack Trace:\n   {0}", validateException.StackTrace);
                Exception ie = validateException.InnerException;
                if (ie != null)
                    {
                    Console.WriteLine("   The Inner Exception:");
                    Console.WriteLine("      Exception Name: {0}", ie.GetType().Name);
                    Console.WriteLine("      Message: {0}\n", ie.Message);
                    Console.WriteLine("      Stack Trace:\n   {0}\n", ie.StackTrace);
                    }

                throw validateException;
                }
            }
        private bool validateCokRiskmasterPfImportRecord(dynamic record)
            {
            string npiNumber = null;
            string title = null;
            if (!this.hasValidNpiNumber(record, ref npiNumber))
                {
                // insert the error string that is now held by npiValidationString
                this.buildRiskMasterPaymentErrorList(record, "Supplier NBR is missing in Entity NPI_NUMBER");
                return false;
                }
            if (!this.hasValidTitle(record, ref title))
                {
                // insert the error string that is now held by npiValidationString
                this.buildRiskMasterPaymentErrorList(record, "Site Codes is missing in Entity Title");
                return false;
                }
            //An NPI_Number  must exist in XXCOK_AP_SUPPLIERS_VW. 
            //Error shows "Supplier NBR is invalid in Entity NPI_NUMBER"
            if (!this.validateSupplierNbr(npiNumber))
                {
                this.buildRiskMasterPaymentErrorList(record, "Supplier NBR is invalid in Entity NPI_NUMBER");
                return false;
                }
            //Pay Site record must exist on  AP_SUPPLIER_SITES_ALL for NPI_Number as Vendor_ID = NPI_Number and Pay_Site_Flag = ‘Y’ and VENDOR_SITE_CODE = upper(Title). 
            //Error shows "Supplier Site Code is invalid in Entity Title"
            if (!this.validateSupplierSiteCode(npiNumber, title))
                {
                this.buildRiskMasterPaymentErrorList(record, "Supplier Site Code is invalid in Entity Title");
                return false;
                }
            return true;
            }
        private void updateCokRiskmasterPfImportRecord(XVar recordWhereClause, string isValid)
            {
            XVar setIsValidColumn = new XVar();
            setIsValidColumn.SetArrayItem("IS_VALID", isValid);
            oracleDBFacade.Update("xxcok.xxcok_rm_import_pf", setIsValidColumn, recordWhereClause);

            }
        private void buildRiskMasterPaymentErrorList(dynamic record, string errorMessage)
            {
            XVar insertErrorRow = buildPfImportRecordPrimaryKeyClause(record);

            insertErrorRow.SetArrayItem("ERROR_DESCR", errorMessage);
            insertErrorList.Add(insertErrorRow);
            }
        private XVar buildPfImportRecordPrimaryKeyClause(dynamic record)
            {
            XVar recordWhereClause = new XVar();
            XVar value;
            if (record.TryGetValue("IMPORT_ID", out value))
                {
                if (value != null && value.IsString() && (value.ToString().Length > 0))
                    {
                    recordWhereClause.SetArrayItem("IMPORT_ID", value);
                    }
                else
                    {
                    throw new NullReferenceException("IMPORT_ID was null in a data retreived from non null column");
                    }
                }
            else
                {
                throw new Exception("Unable to retrieve IMPORT_ID from table xxcok.xxcok_rm_import_pf");
                }
            if (record.TryGetValue("IMPORT_DATE", out value))
                {
                if (value != null && value.Value is DateTime)
                    {
                    DateTime import_datetime = (DateTime)value.Value;
                    recordWhereClause.SetArrayItem("IMPORT_DATE", import_datetime.ToString("yyyyMMdd"));
                    }
                else
                    {
                    throw new NullReferenceException("IMPORT_DATE was null in a data retreived from non null column");
                    }
                }
            else
                {
                throw new Exception("Unable to retrieve IMPORT_DATE from table xxcok.xxcok_rm_import_pf");
                }
            if (record.TryGetValue("PAYMENT_ID", out value))
                {
                if (value != null && value.IsNumeric())
                    {
                    recordWhereClause.SetArrayItem("PAYMENT_ID", value.ToString());

                    }
                else
                    {
                    throw new NullReferenceException("PAYMENT_ID was null in a data retreived from non null column");
                    }
                }
            else
                {
                throw new Exception("Unable to retrieve PAYMENT_ID from table xxcok.xxcok_rm_import_pf");
                }
            return recordWhereClause;
            }
        public bool hasValidNpiNumber(dynamic record, ref string npiValidationString)
            {
            XVar value;
            if (record.TryGetValue("NPI_NUMBER", out value))
                {
                if (value != null && (value.IsString()) && (value.ToString().Length > 0))
                    {
                    npiValidationString = value;
                    return true;
                    }
                }
            return false;
            }
        public bool hasValidTitle(dynamic record, ref string titleValidationString)
            {
            XVar value;
            if (record.TryGetValue("TITLE", out value))
                {
                if ((value != null) && (value.IsString()) && (value.ToString().Length > 0))
                    {
                    titleValidationString = value;
                    return true;
                    }
                }
            return false;
            }
        public bool validateSupplierSiteCode(string npiNumber, string title)
            {
            bool returnSuccess = false;

            try
                {
                string formatSupplierSiteCodeStr = @"SELECT APSSA.VENDOR_ID AS VENDOR_ID
                                                    FROM AP.AP_SUPPLIER_SITES_ALL APSSA
                                                    INNER JOIN AP.AP_SUPPLIERS APS
                                                    ON APSSA.VENDOR_ID = APS.VENDOR_ID
                                                    WHERE APS.SEGMENT1 = {0}
                                                    AND UPPER(APSSA.VENDOR_SITE_CODE) = UPPER('{1}')
                                                    AND APSSA.PAY_SITE_FLAG IS NOT NULL 
                                                    AND APSSA.PAY_SITE_FLAG = 'Y'";
                string validateSupplierSiteCodeStmt = String.Format(formatSupplierSiteCodeStr, npiNumber, title);

                dynamic qresult = oracleDBFacade.Query(validateSupplierSiteCodeStmt);

                XVar recordXVar = qresult.fetchAssoc();

                if (recordXVar != null && recordXVar.Count() > 0)
                    {
                    dynamic record = recordXVar.Value;
                    XVar value;
                    if (record.TryGetValue("VENDOR_ID", out value))
                        {
                        if (value != null)
                            {
                            returnSuccess = true;
                            }
                        }
                    }

                }
            catch (Exception e)
                {
                throw e;
                /*
                Console.WriteLine("An exception ({0}) occurred.",
                                    e.GetType().Name);
                Console.WriteLine("   Message:\n{0}", e.Message);
                Console.WriteLine("   Stack Trace:\n   {0}", e.StackTrace);
                Exception ie = e.InnerException;
                if (ie != null)
                    {
                    Console.WriteLine("   The Inner Exception:");
                    Console.WriteLine("      Exception Name: {0}", ie.GetType().Name);
                    Console.WriteLine("      Message: {0}\n", ie.Message);
                    Console.WriteLine("      Stack Trace:\n   {0}\n", ie.StackTrace);
                    }
                */
                }
            return returnSuccess;
            }
        public bool validateSupplierNbr(string npiNumber)
            {
            bool returnSuccess = false;
            try
                {

                XVar testSelectDictionary = new XVar();
                string formatSupplierNbrStr = @"SELECT APSSA.VENDOR_ID AS VENDOR_ID
                                                    FROM AP.AP_SUPPLIER_SITES_ALL APSSA
                                                    INNER JOIN AP.AP_SUPPLIERS APS
                                                    ON APSSA.VENDOR_ID = APS.VENDOR_ID
                                                    WHERE APS.SEGMENT1 = {0}";
                string validateSupplierNbrStmt = String.Format(formatSupplierNbrStr, npiNumber);

                dynamic qresult = oracleDBFacade.Query(validateSupplierNbrStmt);

                XVar recordXVar = qresult.fetchAssoc();

                if (recordXVar != null && recordXVar.Count() > 0)
                    {
                    dynamic record = recordXVar.Value;
                    XVar value;
                    if (record.TryGetValue("VENDOR_ID", out value))
                        {
                        if (value != null)
                            {
                            returnSuccess = true;
                            }
                        }
                    }

                }
            catch (Exception e)
                {
                throw e;
                /*
                Console.WriteLine("An exception ({0}) occurred.",
                                    e.GetType().Name);
                Console.WriteLine("   Message:\n{0}", e.Message);
                Console.WriteLine("   Stack Trace:\n   {0}", e.StackTrace);
                Exception ie = e.InnerException;
                if (ie != null)
                    {
                    Console.WriteLine("   The Inner Exception:");
                    Console.WriteLine("      Exception Name: {0}", ie.GetType().Name);
                    Console.WriteLine("      Message: {0}\n", ie.Message);
                    Console.WriteLine("      Stack Trace:\n   {0}\n", ie.StackTrace);
                    }
                */
                }
            return returnSuccess;
            }
        private void countValidRiskMasterPayments()
            {
            string selectValidRecordsStatement = @"select count(*) as COUNT
                                    FROM XXCOK.XXCOK_RM_IMPORT_PF PF inner join
                                        (select
                                        IMPORT_ID,
                                        IMPORT_DATE,
                                        PAYMENT_ID,
                                        nvl(max(INVOICE_NUMBER), '0') as INVOICE_NUMBER
                                        from XXCOK.XXCOK_RM_IMPORT_PD
                                        group by IMPORT_ID, IMPORT_DATE, PAYMENT_ID) PD
                                    on PF.IMPORT_ID = PD.IMPORT_ID and
                                    PF.IMPORT_DATE = PD.IMPORT_DATE and
                                    PF.PAYMENT_ID = PD.PAYMENT_ID
                                    where PF.IS_VALID = 1
                                    AND IS_PROCESSED = 0";
            dynamic qresult = oracleDBFacade.Query(selectValidRecordsStatement);

            XVar recordXVar = qresult.fetchAssoc();
            if (recordXVar != null && recordXVar.Count() > 0)
                {
                dynamic record = recordXVar.Value;
                XVar value;
                if (record.TryGetValue("COUNT", out value))
                    {
                    if (value != null && value.IsNumeric())
                        {
                        int count = int.Parse(value);
                        this.TotalRecordsValidated = this.TotalRecordsValidated + count;
                        }
                    }
                }
            }
        private void populateRiskMasterPaymentsTable()
            {
            string insertStatement = @"INSERT INTO XXCOK.XXCOK_RISK_MASTER_PAYMENTS
                                    (CTL_NUMBER,
                                    CLAIM_ID,
                                    CLAIM_NUMBER,
                                    NAME,
                                    ADDR1,
                                    ADDR2,
                                    CITY,
                                    STATE,
                                    ZIP_CODE,
                                    AMOUNT,
                                    TRANS_DATE,
                                    INVOICE_NUMBER,
                                    DEPT,
                                    LINE_OF_BUS_CODE,
                                    SUPPLIER_NBR,
                                    VENDOR_SITE_CODE,
                                    INVOICE_TOTAL,
                                    TRANS_NUMBER)
                                    SELECT 
                                    pf.CONTROL_NUMBER,
                                    pf.CLAIM_ID,
                                    pf.CLAIM_NUMBER,
                                    pf.PAYEE_NAME,
                                    pf.STREET_ADDRESS_1,
                                    pf.STREET_ADDRESS_2,
                                    pf.CITY,
                                    pf.STATE,
                                    pf.ZIP_CODE,
                                    pd.SPLIT_AMOUNT,
                                    pf.TRANSACTION_DATE,
                                    pd.INVOICE_NUMBER,
                                    CASE WHEN SUBSTR(CLAIM_NUMBER, 3, 1) = 'I' THEN SUBSTR(CLAIM_NUMBER,4,5) ELSE SUBSTR(CLAIM_NUMBER,3,5) END AS DEPT,
                                    CASE WHEN SUBSTR(CLAIM_NUMBER, 1, 2) = 'WC' THEN 243
                                    WHEN SUBSTR(CLAIM_NUMBER, 1, 2) = 'VA' THEN 242 
                                    WHEN SUBSTR(CLAIM_NUMBER, 1, 2) = 'GC' THEN 241 
                                    ELSE NULL END AS LINE_OF_BUS_CODE,
                                    pf.NPI_NUMBER,
                                    pf.TITLE,
                                    0,
                                    pf.CHECK_NUMBER
                                    FROM XXCOK.XXCOK_RM_IMPORT_PF PF 
                                    INNER JOIN XXCOK.XXCOK_RM_IMPORT_PD PD
                                        on PF.IMPORT_ID = PD.IMPORT_ID and
                                        PF.IMPORT_DATE = PD.IMPORT_DATE and
                                        PF.PAYMENT_ID = PD.PAYMENT_ID
                                    where PF.is_valid = 1
                                    AND PF.IS_PROCESSED = 0
                                    AND EXISTS (
                                        SELECT NPI_NUMBER FROM XXCOK.XXCOK_RM_VENDOR_DTL_PAY VDP
                                        WHERE pf.NPI_NUMBER = VDP.NPI_NUMBER
                                        AND pf.TITLE = VDP.TITLE )
                                    UNION
                                    SELECT 
                                    pf.CONTROL_NUMBER,
                                    pf.CLAIM_ID,
                                    pf.CLAIM_NUMBER,
                                    pf.PAYEE_NAME,
                                    pf.STREET_ADDRESS_1,
                                    pf.STREET_ADDRESS_2,
                                    pf.CITY,
                                    pf.STATE,
                                    pf.ZIP_CODE,
                                    pf.CHECK_AMOUNT,
                                    pf.TRANSACTION_DATE,
                                    pd.INVOICE_NUMBER,
                                    CASE WHEN SUBSTR(CLAIM_NUMBER, 3, 1) = 'I' THEN SUBSTR(CLAIM_NUMBER,4,5) ELSE SUBSTR(CLAIM_NUMBER,3,5) END AS DEPT,
                                    CASE WHEN SUBSTR(CLAIM_NUMBER, 1, 2) = 'WC' THEN 243
                                    WHEN SUBSTR(CLAIM_NUMBER, 1, 2) = 'VA' THEN 242 
                                    WHEN SUBSTR(CLAIM_NUMBER, 1, 2) = 'GC' THEN 241 
                                    ELSE NULL END AS LINE_OF_BUS_CODE,
                                    pf.NPI_NUMBER,
                                    pf.TITLE,
                                    0,
                                    pf.CHECK_NUMBER
                                    FROM XXCOK.XXCOK_RM_IMPORT_PF PF inner join 
                                    (select 
	                                    IMPORT_ID,
	                                    IMPORT_DATE,
	                                    PAYMENT_ID,
	                                    nvl(max(INVOICE_NUMBER), '0') as INVOICE_NUMBER
	                                    from XXCOK.XXCOK_RM_IMPORT_PD
	                                    group by IMPORT_ID, IMPORT_DATE, PAYMENT_ID) PD
                                    on PF.IMPORT_ID = PD.IMPORT_ID and
                                    PF.IMPORT_DATE = PD.IMPORT_DATE and
                                    PF.PAYMENT_ID = PD.PAYMENT_ID
                                    where PF.is_valid = 1
                                    AND PF.IS_PROCESSED = 0
                                    AND NOT EXISTS (
                                        SELECT NPI_NUMBER FROM XXCOK.XXCOK_RM_VENDOR_DTL_PAY VDP
                                        WHERE ( pf.NPI_NUMBER = VDP.NPI_NUMBER AND pf.TITLE = VDP.TITLE))";
            oracleDBFacade.Exec(insertStatement);

            }
        private void updateRiskMasterPFImportTables()
            {

            string updateStatement = @"UPDATE XXCOK.XXCOK_RM_IMPORT_PF PF 
                                        SET  IS_PROCESSED = 1 
                                        WHERE IS_VALID = 1 AND IS_PROCESSED = 0
                                        AND EXISTS (
                                        SELECT 1 FROM XXCOK.XXCOK_RISK_MASTER_PAYMENTS RMP
                                        WHERE PF.CONTROL_NUMBER = RMP.CTL_NUMBER  AND
                                        PF.CLAIM_ID = RMP.CLAIM_ID AND
                                        PF.CLAIM_NUMBER = RMP.CLAIM_NUMBER)";
            oracleDBFacade.Exec(updateStatement);

            }
        private static bool isNumericType(object o)
            {
            switch (Type.GetTypeCode(o.GetType()))
                {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    return true;
                default:
                    return false;
                }
            }
        }
    }
