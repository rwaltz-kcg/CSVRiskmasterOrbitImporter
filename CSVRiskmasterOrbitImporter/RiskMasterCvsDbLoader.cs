using System;
using System.Collections.Generic;
using System.IO;
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
    public class RiskMasterCvsDbLoader
        {
        OracleDBFacade oracleDBFacade = new OracleDBFacade();
        public RiskMasterCvsDbLoader()
            {
            }
        public void loadPdCsv(IList<XVar> pdCsvList)
            {

            foreach (XVar pdCvsDataRow in pdCsvList)
                {
                oracleDBFacade.Insert("xxcok.xxcok_rm_import_pd", pdCvsDataRow);
                }

            }
        public void loadPfCsv(IList<XVar> pfCsvList)
            {

            foreach (XVar pfCvsDataRow in pfCsvList)
                {
                oracleDBFacade.Insert("xxcok.xxcok_rm_import_pf", pfCvsDataRow);
                }

            }
        public void removePfCsv(IList<XVar> pfCsvList)
            {

            foreach (XVar pfCvsDataRow in pfCsvList)
                {
                try
                    {
                    oracleDBFacade.Delete("xxcok.xxcok_rm_import_pf", pfCvsDataRow);
                    }
                catch (Exception ex)
                    {
                    Console.WriteLine(ex.Message);
                    }
                }

            }
        public void removePdCsv(IList<XVar> pdCsvList)
            {

            foreach (XVar pdCvsDataRow in pdCsvList)
                {
                try
                    {
                    oracleDBFacade.Delete("xxcok.xxcok_rm_import_pd", pdCvsDataRow);
                    }
                catch (Exception ex)
                    {
                    Console.WriteLine(ex.Message);
                    }
                }

            }

        }
    }