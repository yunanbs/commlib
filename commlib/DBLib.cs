using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using Oracle.DataAccess.Client;
using Npgsql;
using System.Data.SqlClient;
using System.Data.OleDb;

namespace Com.Baosight.Yu
{

    /// <summary>
    /// 2016年8月2日
    /// 数据库操作辅助结构
    /// </summary>
    public struct DBOPStruct
    {
        public int IResult;
        public DataTable DTResult;
        public string ErrMsg;
    }

    /// <summary>
    /// 2016年8月2日
    /// 数据库助手
    /// </summary>
    public static class DBHelper
    {
        /// <summary>
        /// sql语句处理助手
        /// </summary>
        /// <param name="dbstr">数据库连接字符串</param>
        /// <param name="dbtype">数据库类型 PostgreSql = 0, Oracle, SqlServer, Access, Excel</param>
        /// <param name="sql">需要执行的单句sql</param>
        /// <param name="sqls">需要事务处理的sqls</param>
        /// <param name="datasql">用来获取数据的查询语句</param>
        /// <returns>数据库操作结构</returns>
        public static DBOPStruct excuteql(string dbstr, int dbtype, string sql = "", string[] sqls = null, string datasql = "")
        {
            DBOPStruct result = new DBOPStruct();//定义返回结构
            using (DBLib tmpdb = new DBLib(dbstr, (DBLib.DataBaseType)dbtype))//创建数据库链接
            {
                tmpdb.IniDB(out result.IResult, out result.ErrMsg);//初始化数据库
                if (string.IsNullOrEmpty(result.ErrMsg))//测试数据库连接是否正常
                {
                    if (!string.IsNullOrEmpty(sql))
                        tmpdb.ExcuteSql(sql, out result.IResult, out result.ErrMsg);//执行单句sql语句

                    if (sqls != null)
                        tmpdb.ExcuteSqls(sqls, out result.IResult, out result.ErrMsg);//事务批量执行语句

                    if (!string.IsNullOrEmpty(datasql))
                        tmpdb.GetDataTable(datasql, out result.DTResult, out result.ErrMsg);//执行查询语句 返回datatable
                }
            };

            return result;
        }

        /// <summary>
        /// 测试数据库链接
        /// </summary>
        /// <param name="dbstr"></param>
        /// <param name="dbtype"></param>
        /// <returns></returns>
        public static DBOPStruct testdb(string dbstr, int dbtype)
        {
            DBOPStruct result = new DBOPStruct();
            using (DBLib tmpdb = new DBLib(dbstr, (DBLib.DataBaseType)dbtype))
            {
                tmpdb.IniDB(out result.IResult, out result.ErrMsg);//初始化数据库
            };
            return result;
        }
    }

    /// <summary>
    /// Create by yn 2016-03-11
    /// 数据库操作类，支持数据库参见数据库枚举类型
    /// 提供事务操作方式
    /// 短链接方式，即用即弃
    /// Oracle 采用Oracle官方驱动
    /// SqlServer 采用.net自带驱动
    /// PostgreSql 采用PostgreSql官方驱动
    /// 其他格式采用Oledb驱动
    /// DB2 暂时不支持 需要时再行添加
    /// </summary>
    public class DBLib:IDisposable
    {
        //数据库类型
        public enum DataBaseType { PostgreSql = 0, Oracle, SqlServer, Access, Excel, none };

        public string DBHost { get; set; }//数据库服务地址
        public string DBName { get; set; }//数据库名称
        public string DBUserName { get; set; }//数据库用户名
        public string DBPassword { get; set; }//数据库密码
        public DataBaseType DBType {get;set;}//数据库类型
        public string ConnectStr { get; set; }//连接字符串

        //数据库对象接口
        IDbConnection _IdbCon = null;
        IDbCommand _IdbCommand = null;
        IDbDataAdapter _IdbDataAdapter = null;
        IDbTransaction _IdbTransaction = null;

        /// <summary>
        /// 带参数初始化方法
        /// </summary>
        /// <param name="Host">数据库地址</param>
        /// <param name="DataBase">数据库名称</param>
        /// <param name="UserName">数据库用户名</param>
        /// <param name="Password">数据库密码</param>
        /// <param name="DBFlag">数据库连接标记</param>
        public DBLib(string Host,string DataBase,string UserName,string Password,int DBFlag)
        {
            this.DBHost = Host;
            this.DBName = DataBase;
            this.DBUserName = UserName;
            this.DBPassword = Password;
            this.DBType = (DataBaseType)DBFlag;
        }

        public DBLib(string Host, string DataBase, string UserName, string Password, DataBaseType DBFlag)
        {
            this.DBHost = Host;
            this.DBName = DataBase;
            this.DBUserName = UserName;
            this.DBPassword = Password;
            this.DBType = DBFlag;
        }

        /// <summary>
        /// 添加对连接字符串的支持  可以直接使用连接字符串进行配置
        /// </summary>
        /// <param name="ConnectString"></param>
        /// <param name="DBFlag"></param>
        public DBLib(string ConnectString, int DBFlag)
        {
            this.ConnectStr = ConnectString;
            this.DBType = (DataBaseType)DBFlag;
        }

        public DBLib(string ConnectString, DataBaseType DBFlag)
        {
            this.ConnectStr = ConnectString;
            this.DBType = DBFlag;
        }

        /// <summary>
        /// 初始化数据库连接
        /// </summary>
        /// <param name="Result">1 测试通过|-1 测试失败</param>
        /// <param name="ErrMessage">异常信息</param>
        public void IniDB(out int Result,out string ErrMessage)
        {
            Result = 0;
            ErrMessage = string.Empty;
            switch (this.DBType)
            {
                case DataBaseType.Oracle:
                    if (string.IsNullOrEmpty(this.ConnectStr))
                    {
                        this.ConnectStr = string.Format("Data Source={0};User Id={1};Password={2};", this.DBName, this.DBUserName, this.DBPassword);
                    }
                    _IdbCon = new OracleConnection(this.ConnectStr);
                    _IdbCommand = new OracleCommand();
                    _IdbDataAdapter = new OracleDataAdapter();
                    break;

                case DataBaseType.PostgreSql:
                    if (string.IsNullOrEmpty(this.ConnectStr))
                    {
                        this.ConnectStr = string.Format("Server={0};Port={1};User Id={2};Password={3};Database={4}", this.DBHost.Split(':')[0], this.DBHost.Split(':')[1], this.DBUserName, this.DBPassword, this.DBName);
                    }
                    _IdbCon = new NpgsqlConnection(this.ConnectStr);
                    _IdbCommand = new NpgsqlCommand();
                    _IdbDataAdapter = new NpgsqlDataAdapter();
                    break;

                case DataBaseType.SqlServer:
                    if (string.IsNullOrEmpty(this.ConnectStr))
                    {
                        this.ConnectStr = string.Format("Data Source={0};Initial Catalog={1};User ID={2};Password={3};", this.DBHost, this.DBName, this.DBUserName, this.DBPassword);
                    }
                    _IdbCon = new SqlConnection(this.ConnectStr);
                    _IdbCommand = new SqlCommand();
                    _IdbDataAdapter = new SqlDataAdapter();
                    break;

                case DataBaseType.Access:
                    if (string.IsNullOrEmpty(this.ConnectStr))
                    {
                        this.ConnectStr = string.Format("Provider = Microsoft.Jet.OleDb.4.0; Data Source = {0}", this.DBName);
                    }
                    _IdbCon = new OleDbConnection(this.ConnectStr);
                    _IdbCommand = new OleDbCommand();
                    _IdbDataAdapter = new OleDbDataAdapter();
                    break;

                case DataBaseType.Excel:
                    if (string.IsNullOrEmpty(this.ConnectStr))
                    {
                        this.ConnectStr = string.Format("Provider=Microsoft.Ace.OLEDB.12.0;Data Source='{0}';Extended Properties='Excel 8.0;HDR=NO;IMEX=1';", this.DBName);
                    }
                    _IdbCon = new OleDbConnection(this.ConnectStr);
                    _IdbCommand = new OleDbCommand();
                    _IdbDataAdapter = new OleDbDataAdapter();
                    break;
            }
           
            try
            {
                _IdbCon.Open();
                Result = 1;
            }catch(Exception ex)
            {
                Result = -1;
                ErrMessage = ex.Message;
            }
            finally
            {
                _IdbCon.Close();
            }
        }

        /// <summary>
        /// 执行单条Sql语句
        /// </summary>
        /// <param name="s_sql">需要执行的Sql语句</param>
        /// <param name="Result">语句影响的记录数量|-1 语句失败</param>
        /// <param name="ErrMessage">异常信息</param>
        public void ExcuteSql(string sql,out int Result,out string ErrMessage)
        {
            Result = 0;
            ErrMessage = string.Empty;
            try
            {
                if (_IdbCon.State != ConnectionState.Open)
                {
                    _IdbCon.Open();
                }
                _IdbCommand.CommandType = CommandType.Text;
                _IdbCommand.Connection = _IdbCon;
                _IdbCommand.CommandText = sql;
                Result = _IdbCommand.ExecuteNonQuery();
            }
            catch(Exception ex)
            {
                Result = -1;
                ErrMessage = ex.Message;
            }
            finally
            {
                _IdbCon.Close();
            }
        }

        /// <summary>
        /// 以事务方式处理sql语句 失败则回滚所有操作
        /// </summary>
        /// <param name="sqls">需要事务处理的语句数组</param>
        /// <param name="Result">语句影响的记录数量|-1 事务失败</param>
        /// <param name="ErrMessage">异常信息</param>
        /// <returns></returns>
        public void ExcuteSqls(string[] sqls, out int Result, out string ErrMessage)
        {
            Result = 0;
            ErrMessage = string.Empty;
            try
            {
                if (_IdbCon.State != ConnectionState.Open)
                {
                    _IdbCon.Open();
                }
                _IdbCommand.CommandType = CommandType.Text;
                _IdbCommand.Connection = _IdbCon;
                _IdbTransaction = _IdbCon.BeginTransaction();
                _IdbCommand.Transaction = _IdbTransaction;
                foreach (string s_sql in sqls)
                {
                    _IdbCommand.CommandText = s_sql;
                    Result += _IdbCommand.ExecuteNonQuery();
                }
                _IdbTransaction.Commit();
            }
            catch (Exception ex)
            {
                try
                {
                    _IdbTransaction.Rollback();//回滚
                    ErrMessage = string.Format("操作已经回滚，异常原因:{0}", ex.Message);
                }
                catch 
                {
                    if (_IdbTransaction.Connection != null)
                    {
                        ErrMessage = string.Format("操作回滚失败，异常原因:{0}", ex.Message);
                    }
                }
                Result = -1;
            }
            finally
            {
                _IdbCon.Close();
            }

        }

        /// <summary>
        /// 分包处理数据库操作 操作失败则按照分包粒度进行回滚
        /// </summary>
        /// <param name="sqls">需要执行的Sql语句数组</param>
        /// <param name="packsize">单个分包的大小</param>
        /// <param name="Result">每个分包操作影响的记录数量</param>
        /// <param name="ErrMessage">每个分包的异常信息</param>
        public void ExcuteSqls(string[] sqls, int packsize, out int[] Result, out string[] ErrMessage)//分包处理大批量数据
        {
            Result = null;
            ErrMessage = null;

            List<int> l_Result = new List<int>();
            List<string> l_ErrMessage = new List<string>();

            List<string> l_subsqls = null;
            while (sqls.Length > packsize)
            {
                l_subsqls = null;
                l_subsqls = sqls.Take(packsize).ToList();
                sqls = sqls.Skip(packsize).ToArray();

                int i_Result = 0;
                string s_ErrMessage = string.Empty;
                this.ExcuteSqls(l_subsqls.ToArray(), out i_Result, out s_ErrMessage);
                l_Result.Add(i_Result);
                l_ErrMessage.Add(s_ErrMessage); 
            }

            if (sqls.Length > 0)
            {
                int i_Result = 0;
                string s_ErrMessage = string.Empty;
                this.ExcuteSqls(sqls, out i_Result, out s_ErrMessage);
                l_Result.Add(i_Result);
                l_ErrMessage.Add(s_ErrMessage);
            }
        }

        /// <summary>
        /// 获取数据 DataTable方式
        /// </summary>
        /// <param name="sql">查询语句</param>
        /// <param name="Result">结果数据集</param>
        /// <param name="ErrMessage">异常信息</param>
        public void GetDataTable(string sql,out DataTable Result,out string ErrMessage)
        {
            DataSet ds_tmp = new DataSet();
            Result = null;
            ErrMessage = string.Empty;
            try
            {
                if (_IdbCon.State != ConnectionState.Open)
                {
                    _IdbCon.Open();
                }
                _IdbCommand.CommandType = CommandType.Text;
                _IdbCommand.Connection = _IdbCon;
                _IdbCommand.CommandText = sql;
                _IdbDataAdapter.SelectCommand = _IdbCommand;
                _IdbDataAdapter.Fill(ds_tmp);
                Result = ds_tmp.Tables[0];
            }
            catch(Exception ex)
            {
                ErrMessage = ex.Message;
            }
            finally
            {
                _IdbCon.Close();
            }
        }

        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="s_proname">存储过程名称</param>
        /// <param name="o_params">参数</param>
        /// <param name="Result">存储过程影响的记录数</param>
        /// <param name="ErrMessage">异常信息</param>
        public void ExcutePro(string s_proname, object[] o_params, out int Result, out string ErrMessage)
        {
            Result = 0;
            ErrMessage = string.Empty;
            try
            {
                if (_IdbCon.State != ConnectionState.Open)
                {
                    _IdbCon.Open();
                }
                _IdbCommand.CommandType = CommandType.StoredProcedure;
                _IdbCommand.CommandText = s_proname;
                _IdbCommand.Connection = _IdbCon;
                foreach (object o_param in o_params)
                {
                    _IdbCommand.Parameters.Add(o_param);
                }
                Result = _IdbCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                ErrMessage = ex.Message;
            }
            finally
            {
                _IdbCon.Close();
            }
        }

        public void Dispose()
        {
            if (_IdbCon != null)
            {
                _IdbCommand = null;
                _IdbDataAdapter = null;
                _IdbTransaction = null;
                _IdbCon.Close();
                _IdbCon.Dispose();
            }
        }
    }
}
