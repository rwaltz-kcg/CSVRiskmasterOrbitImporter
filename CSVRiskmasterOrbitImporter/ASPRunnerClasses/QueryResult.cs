using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using System.Reflection;
using runnerDotNet;
namespace runnerDotNet
{
	public partial class QueryResult : XClass
	{
		protected dynamic connectionObj;
		protected dynamic handle;
		protected dynamic data;
		protected dynamic fieldNames = XVar.Array();
		protected dynamic upperMap = XVar.Array();
		protected dynamic fieldMap = XVar.Array();
		protected dynamic state = XVar.Pack(-1);
		public QueryResult(dynamic _param_connectionObj, dynamic _param_qHandle)
		{
			#region pass-by-value parameters
			dynamic connectionObj = XVar.Clone(_param_connectionObj);
			dynamic qHandle = XVar.Clone(_param_qHandle);
			#endregion

			this.connectionObj = XVar.Clone(connectionObj);
			this.handle = XVar.Clone(qHandle);
		}
		public virtual XVar getQueryHandle()
		{
			return this.handle;
		}
		public virtual XVar fetchAssoc()
		{
			dynamic ret = null;
			if(this.state == 1)
			{
				return null;
			}
			if(this.state == 0)
			{
				this.state = XVar.Clone(-1);
				return numericToAssoc((XVar)(this.data));
			}
			ret = XVar.Clone(fetch_array((XVar)(this.handle)));
			this.state = XVar.Clone((XVar.Pack(ret) ? XVar.Pack(-1) : XVar.Pack(1)));
			return ret;
		}

		public virtual XVar numFields()
		{
			return this.connectionObj.num_fields((XVar)(this.handle));
		}
		public virtual XVar fieldName(dynamic _param_offset)
		{
			#region pass-by-value parameters
			dynamic offset = XVar.Clone(_param_offset);
			#endregion

			return this.connectionObj.field_name((XVar)(this.handle), (XVar)(offset));
		}
		public XVar fetch_array(dynamic qHanle)
		{
			//db_fetch_array
			if (qHanle != null)
			{
				RunnerDBReader reader = (RunnerDBReader)qHanle;
				if (reader.Read())
				{
					XVar result = new XVar();
					for (int i = 0; i < reader.FieldCount; i++)
					{
						result.SetArrayItem(reader.GetName(i), reader[i]);
					}
					return result;
				}
			}

			return XVar.Array();
		}

		protected virtual XVar numericToAssoc(dynamic _param_data)
		{
			#region pass-by-value parameters
			dynamic data = XVar.Clone(_param_data);
			#endregion

			dynamic i = null, nFields = null, ret = XVar.Array();
			ret = XVar.Clone(XVar.Array());
			nFields = XVar.Clone(numFields());
			i = new XVar(0);
			for(;i < nFields; ++(i))
			{
				ret.InitAndSetArrayItem(data[i], this.fieldNames[i]);
			}
			return ret;
		}

	}
}
